from PIL import Image
import numpy as np

# Load image
img = Image.open("/Users/nuthan/Documents/Property/_RESEARCH/Google Earth assets/Screenshot 2025-12-29 003022.png").convert("RGBA")
data = np.array(img)

# Remove white / near-white background
r = data[..., 0]
g = data[..., 1]
b = data[..., 2]
white_pixels = (r > 200) & (g > 200) & (b > 200)
data[white_pixels] = 0

# Save output
output = Image.fromarray(data)
output.save("/Users/nuthan/Documents/Property/_RESEARCH/Google Earth assets/RRR_Radial_Roads.png")
