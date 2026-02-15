from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
from azure.ai.agents.models import ListSortOrder, FilePurpose, MessageBlockType, MessageAttachment

project = AIProjectClient(
    endpoint="https://nuthan-ai-resource.services.ai.azure.com/api/projects/nuthan-ai",
    credential=DefaultAzureCredential(),
)

# Upload file and create vector store
# file = project.agents.files.upload(file_path="./product_info_1.md", purpose=FilePurpose.AGENTS)
# vector_store = project.agents.vector_stores.create_and_poll(file_ids=[file.id], name="my_vectorstore")

# Create file search tool and agent
# file_search = FileSearchTool(vector_store_ids=[vector_store.id])
agent = project.agents.create_agent(
    model="gpt-4o-mini",
    name="NutritionLabelOCRAgent",
    instructions="You are a Nutrition Label Reader. You extract the information from the nutrition label image /"
                 " the text message given and return it as a table. Always display per 100gm.",
    # tools=file_search.definitions,
    # tool_resources=file_search.resources,
)

# MessageBlockType.IMAGE_FILE

thread = project.agents.threads.create()
image_file = project.agents.files.upload(file_path="./nutrilabel.jpg", purpose=FilePurpose.AGENTS)
attachment = MessageAttachment(tools=agent.tools,  # Attach the tools to the message
                               file_id=image_file.id,
                               type=MessageBlockType.IMAGE_FILE,
                               description="Nutrition label image")
message = project.agents.messages.create(
    thread_id=thread.id,
    role="user",
    content="Hi NutritionLabelReader, please read the nutrition label from the image below.",
    attachments=[attachment])

run = project.agents.runs.create_and_process(thread_id=thread.id, agent_id=agent.id)
if run.status == "failed":
    # Check if you got "Rate limit is exceeded.", then you want to get more quota
    print(f"Run failed: {run.last_error}")

# Get messages from the thread
messages = project.agents.messages.list(thread_id=thread.id)

# Get the last message from the sender
messages = project.agents.messages.list(thread_id=thread.id, order=ListSortOrder.ASCENDING)
for message in messages:
    if message.run_id == run.id and message.text_messages:
        print(f"{message.role}: {message.text_messages[-1].text.value}")

# Cleanup resources
# project.agents.vector_stores.delete(vector_store.id)
# project.agents.files.delete(file_id=file.id)
project.agents.delete_agent(agent.id)
print("Deleted agent")