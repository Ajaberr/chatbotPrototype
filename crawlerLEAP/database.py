import weaviate
from weaviate.classes.init import Auth
import os
from weaviate.classes.config import Configure




client = weaviate.connect_to_weaviate_cloud(
    cluster_url= "https://8fzaavbvs9cmcmmhj3r9ag.c0.us-east1.gcp.weaviate.cloud",                                    # Replace with your Weaviate Cloud URL
    auth_credentials=Auth.api_key("UO94NG8zO65WQ7TYe6RzjbKNIwepH4TvAMa1"),             # Replace with your Weaviate Cloud key
)

print(client.is_ready())  # Should print: `True`

storage = client.collections.create(
    name="leapData",
    vectorizer_config=Configure.Vectorizer.text2vec_cohere(),   # Configure the Cohere embedding integration
    generative_config=Configure.Generative.cohere()             # Configure the Cohere generative AI integration
)


client.close()  # Free up resources