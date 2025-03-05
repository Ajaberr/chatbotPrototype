import streamlit as st
import requests
import json
from urllib.parse import urljoin, urlparse

import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure, Integrations
from weaviate.classes.query import MetadataQuery

# ------------------------------------------------------------------------
# Page configuration and styling
# ------------------------------------------------------------------------
st.set_page_config(
    page_title="LEAP Research Assistant",
    page_icon="🌍",
    layout="wide"
)

# ------------------------------------------------------------------------
# OpenRouter API configuration
# ------------------------------------------------------------------------
DEEPSEEK_MODEL = "deepseek/deepseek-r1:free"
MISTRAL_MODEL = "mistralai/mistral-7b-instruct:free"

API_KEY = "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"
API_URL = "https://openrouter.ai/api/v1/chat/completions"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

# ------------------------------------------------------------------------
# Connect to Weaviate Cloud and configure integrations
# ------------------------------------------------------------------------
@st.cache_resource
def setup_weaviate_client():
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url="https://8fzaavbvs9cmcmmhj3r9ag.c0.us-east1.gcp.weaviate.cloud",  # Replace with your Weaviate Cloud URL
        auth_credentials=Auth.api_key("UO94NG8zO65WQ7TYe6RzjbKNIwepH4TvAMa1"),         # Replace with your Weaviate Cloud key
    )
    integrations = [
        Integrations.cohere(
            api_key="vaJnTfyc9YMXCBGen8l0ttVqt5O0RUrOIIaiIshg",
        ),
    ]
    client.integrations.configure(integrations)
    return client

# Initialize client and get collection
client = setup_weaviate_client()
storage = client.collections.get("leapData")

# ------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------
def call_openrouter(model, system_prompt, user_prompt, max_tokens=1500):
    """Call the OpenRouter API with the specified model, system prompt, user prompt, and parameters."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.3,  # Lower temperature for more factual responses
    }
    
    try:
        with st.spinner(f"Waiting for {model} response..."):
            response = requests.post(API_URL, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def enhance_query(original_query, chosen_model):
    """Enhance the query to improve semantic search results."""
    system_prompt = """
    You are a search query enhancer for a climate science database called LEAP 
    (Learning the Earth with Artificial Intelligence and Physics).

    Your task is to enhance this search query for a semantic search engine that 
    searches through climate science resources.
    
    1. Identify the key climate science concepts, entities, and relationships
    2. Expand climate science abbreviations if needed (e.g., CMIP6, GCM, RCP)
    3. Add relevant climate science synonyms or related terms that might help the search
    4. Format as a clear, concise query that preserves the original intent
    5. Return ONLY the enhanced query, no explanations
    """
    
    response = call_openrouter(
        chosen_model, 
        system_prompt, 
        original_query,
        max_tokens=2000
    )
    
    if isinstance(response, dict) and "choices" in response and response["choices"]:
        enhanced = response["choices"][0]["message"].get("content", original_query)
        return enhanced.strip()
    return original_query

def search_weaviate(query, limit=10):
    """Search the Weaviate database with the given query."""
    try:
        query_result = storage.query.near_text(
            query=query,
            limit=limit,
            return_metadata=MetadataQuery(distance=True)
        )
        return query_result
    except Exception as e:
        st.error(f"Error querying Weaviate: {e}")
        return None

def reduce_text(text, max_chars=5000):
    """
    Reduces the text content by truncating it to a maximum number of characters.
    """
    if text and len(text) > max_chars:
        return text[:max_chars] + "..."
    return text

def format_context(query_result, top_results=5, max_distance=1.0):
    """
    Format the search results into a context for the model.
    Checks for a "url" property and, if missing or empty, falls back to "title" or "question".
    Uses "transcript" if available; otherwise "answer". Content is truncated with reduce_text().
    """
    if not query_result or not query_result.objects:
        return None
        
    combined_blocks = []
    seen_sources = set()
    
    for obj in query_result.objects:
        source = obj.properties.get("url")
        if not source or source.strip() == "":
            source = obj.properties.get("title") or obj.properties.get("question", "Unknown Source")
        
        # Avoid duplicate sources
        if source in seen_sources:
            continue
        seen_sources.add(source)
        
        content = obj.properties.get("transcript") or obj.properties.get("answer")
        if not content or content.startswith("Error scraping"):
            continue

        content = reduce_text(content, max_chars=5000)
        
        source_info = f"--- SOURCE: {source} (relevance: {1 - obj.metadata.distance:.2f}) ---"
        combined_blocks.append(f"{source_info}\n{content}")
    
    if not combined_blocks:
        return None
        
    return "\n\n".join(combined_blocks)

# ------------------------------------------------------------------------
# UI Components
# ------------------------------------------------------------------------
def render_sidebar():
    with st.sidebar:
        st.title("⚙️ Settings")
        
        # Provide both models in a selectbox
        model_choice = st.selectbox(
            "Select AI model:",
            [DEEPSEEK_MODEL, MISTRAL_MODEL],
            index=0,
            help="Select which AI model to use for answering your questions."
        )
        
        # Hardcoded best parameters
        search_limit = 5
        top_results = 5
        max_distance = 0.5
        
        st.divider()
        st.markdown("""
        ### About
        This app semantically searches through LEAP resources and uses AI to provide accurate answers based on the retrieved information.
        
        ### How it works:
        1. Your query is enhanced for better semantic search
        2. Relevant content is retrieved from our database
        3. An AI model uses this content to answer your question
        """)
        show_debug = st.checkbox("Enable Debug Mode", value=False)
    return model_choice, search_limit, top_results, max_distance, show_debug

def render_main_panel():
    st.title("🌍 LEAP Research Assistant")
    st.write("Ask questions about climate data, LEAP resources, or specific datasets.")
    user_query = st.text_input(
        "Enter your question:",
        placeholder="e.g., What gridded climate datasets are available for Australia?"
    )
    return user_query

def render_debug_info(enhanced_query, context, raw_response, query_result=None):
    with st.expander("🔍 Debug Information", expanded=True):
        st.subheader("Enhanced Query")
        st.write(enhanced_query)
        st.subheader("Context Provided to Model")
        if context:
            st.text_area("Retrieved Content", context, height=200)
        else:
            st.error("No relevant context found in database")
        
        if query_result and query_result.objects:
            st.subheader("All Search Results")
            for idx, obj in enumerate(query_result.objects):
                source = obj.properties.get("url")
                if not source or source.strip() == "":
                    source = obj.properties.get("title") or obj.properties.get("question", "Unknown Source")
                st.markdown(f"**Result {idx+1}:** {source}")
                st.markdown(f"**Distance:** {obj.metadata.distance:.4f} (Relevance: {1 - obj.metadata.distance:.4f})")
                content = obj.properties.get("transcript") or obj.properties.get("answer", "")
                if content:
                    preview = content[:200] + "..." if len(content) > 200 else content
                    st.text_area(f"Content Preview {idx+1}", preview, height=100)
                else:
                    st.warning("No content available")
                st.markdown("---")
                
        st.subheader("Raw API Response")
        st.json(raw_response)

def render_answer(response_data):
    if isinstance(response_data, dict) and "choices" in response_data:
        if response_data["choices"] and "message" in response_data["choices"][0]:
            final_answer = response_data["choices"][0]["message"].get("content", "")
            st.markdown("### Answer")
            st.markdown(final_answer)
            col1, col2 = st.columns(2)
            with col1:
                st.button("👍 Helpful")
            with col2:
                st.button("👎 Not Helpful")
        else:
            st.error("No answer content found in the response.")
    elif "error" in response_data:
        st.error(f"Error: {response_data['error']}")
    else:
        st.error("Unrecognized response format.")

def inspect_database():
    with st.expander("🔍 Inspect Database Content", expanded=False):
        st.subheader("Sample Entries")
        try:
            sample = storage.query.fetch_objects(limit=5)
            if not sample or not sample.objects:
                st.warning("No entries found in the database.")
                return
            for idx, obj in enumerate(sample.objects):
                st.markdown(f"**Sample {idx+1}**")
                source = obj.properties.get("url")
                if not source or source.strip() == "":
                    source = obj.properties.get("title") or obj.properties.get("question", "Unknown Source")
                st.markdown(f"**Source:** {source}")
                transcript = obj.properties.get("transcript") or obj.properties.get("answer", "")
                if transcript:
                    preview = transcript[:200] + "..." if len(transcript) > 200 else transcript
                    st.text_area(f"Content Preview {idx+1}", preview, height=100)
                else:
                    st.warning("No content found.")
                st.markdown("---")
        except Exception as e:
            st.error(f"Error inspecting database: {e}")

def test_search():
    with st.expander("🔎 Test Direct Search", expanded=False):
        st.subheader("Test Search Query")
        test_query = st.text_input("Enter test query:")
        if test_query and st.button("Run Test Search"):
            try:
                query_result = search_weaviate(test_query, limit=5)
                if not query_result or not query_result.objects:
                    st.warning("No results found.")
                    return
                st.markdown("### Search Results")
                for idx, obj in enumerate(query_result.objects):
                    source = obj.properties.get("url")
                    if not source or source.strip() == "":
                        source = obj.properties.get("title") or obj.properties.get("question", "Unknown Source")
                    st.markdown(f"**Result {idx+1}:** {source}")
                    st.markdown(f"**Distance:** {obj.metadata.distance:.4f} (Relevance: {1 - obj.metadata.distance:.4f})")
                    content = obj.properties.get("transcript") or obj.properties.get("answer", "")
                    if content:
                        preview = content[:200] + "..." if len(content) > 200 else content
                        st.text_area(f"Content Preview {idx+1}", preview, height=100)
                    else:
                        st.warning("No content available for this result")
                    st.markdown("---")
            except Exception as e:
                st.error(f"Error testing search: {e}")

def main():
    if 'user_query' not in st.session_state:
        st.session_state.user_query = ""
    
    model_choice, search_limit, top_results, max_distance, show_debug = render_sidebar()
    entered_query = render_main_panel()
    if entered_query:
        st.session_state.user_query = entered_query
    
    if show_debug:
        inspect_database()
        test_search()
    
    if st.session_state.user_query:
        user_query = st.session_state.user_query
        if st.button("Search", type="primary"):
            with st.spinner("Enhancing query for climate science context..."):
                enhanced_query = enhance_query(user_query, model_choice)
            
            with st.spinner("Searching LEAP database..."):
                query_result = search_weaviate(query=enhanced_query, limit=search_limit)
            
            context = format_context(query_result, top_results=top_results, max_distance=max_distance)
            
            if context:
                system_prompt = f"""
                You are a helpful assistant for users of the LEAP (Learning the Earth with Artificial Intelligence and Physics) Columbia, which focuses on climate science research.
                
                Answer the user's question using the following LEAP resource content:
                
                {context}
                
                Instructions:
                1. Be factual, comprehensive, and specific using the provided context. 
                2. If the user asks for information not in the context, say "I don't have specific information about that in the LEAP resources I can access."
                3. If you reference a website, include its source information in parentheses.
                4. If you reference a youtube video, include its link and the exact time frames to watch by appending the end of the url with: &t=XhYmZs. "X" is the number of hours, "Y" is the number of minutes, and “Z” is the number of seconds.
                5. Format your response clearly and give detailed answers.
                6. Do not make up information or cite sources not in the provided context.
                7. Focus on climate science information from the provided context.
                8. ALWAYS REFERENCE ATLEAST THREE RESOURCES WITH LINKS TO THE WEBSITE! THIS IS A MUST.
                """
            else:
                system_prompt = """
                You are a helpful assistant for users of the LEAP (Learning the Earth with Artificial Intelligence and Physics) Columbia, which focuses on climate science research.

                Unfortunately, I couldn't find specific information about that topic in the LEAP resources I have access to.
                
                Explain to the user that:
                1. You don't have information about their specific query in the LEAP climate science database.
                2. They should consider rephrasing their question to focus on LEAP-related resources.
                3. Suggest more specific climate science related topics they might ask about.
                """
            
            response_data = call_openrouter(model_choice, system_prompt, user_query)
            render_answer(response_data)
            
            if show_debug:
                render_debug_info(enhanced_query, context, response_data, query_result)

if __name__ == "__main__":
    main()
