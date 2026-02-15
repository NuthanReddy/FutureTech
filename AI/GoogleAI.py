import os
from google import genai
from PIL import Image

# Get API key from environment variable
API_KEY = os.getenv('GOOGLE_API_KEY')
if not API_KEY:
    raise ValueError('Missing GOOGLE_API_KEY environment variable. Please set it to your Google AI API key.')

client = genai.Client(api_key=API_KEY)

image = Image.open("/Users/nuthan/Documents/Property/_RESEARCH/Google Earth assets/Screenshot 2025-12-29 003022.png")

response = client.models.generate_content(model="gemini-2.5-flash-image",
                                         contents=['Remove white background from the given image and export it as a png '
                                                'with transparant background.', image])

for part in response.parts:
    if part.text is not None:
        print(part.text)
    elif part.inline_data is not None:
        image = part.as_image()
        image.save("/Users/nuthan/Documents/Property/_RESEARCH/Google Earth assets/RRR_Radial_Roads.png")
