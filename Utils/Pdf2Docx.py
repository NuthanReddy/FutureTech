from pdf2docx import Converter

pdf_file = '<>.pdf'
docx_file = '<>.docx'

# convert pdf to docx
cv = Converter(pdf_file)
cv.convert(docx_file)      # all pages by default
cv.close()
