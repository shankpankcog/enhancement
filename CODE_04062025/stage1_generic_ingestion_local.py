!pip install pymupdf pymupdf4llm


import fitz  # PyMuPDF
import pymupdf4llm
from typing import Tuple, List, Dict

def get_pdf_bytes_from_local(file_path: str) -> bytes:
    """
    Read a PDF file from the local filesystem and return its bytes.
    """
    with open(file_path, "rb") as f:
        return f.read()

def extract_text_from_pdf(file_path: str) -> Tuple[str, List[Dict]]:
    """
    Extract text from a local PDF file and return structured page-level data.
    """
    try:
        pdf_bytes = get_pdf_bytes_from_local(file_path)
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = pymupdf4llm.to_markdown(doc=doc, page_chunks=True)

        data = [
            {
                "page_number": page["metadata"]["page"],
                "page_text": page["text"],
                "total_pages": page["metadata"]["page_count"]
            }
            for page in pages
        ]
        return "COMPLETED", data

    except Exception as e:
        return "FAILED", [{
            "page_number": -1,
            "page_text": f"Error: {str(e)}",
            "total_pages": -1
        }]

if __name__ == "__main__":
    file_path = r"C:\path\to\your\document.pdf"  # Update this path
    status, result = extract_text_from_pdf(file_path)
    
    if status == "COMPLETED":
        for page in result:
            print(f"Page {page['page_number']}/{page['total_pages']}:")
            print(page['page_text'][:300], "...\n")
    else:
        print("Failed to extract PDF:", result)
