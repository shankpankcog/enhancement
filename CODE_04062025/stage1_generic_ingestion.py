!pip install boto3 pymupdf pymupdf4llm

import boto3
import fitz  # PyMuPDF
import pymupdf4llm
from typing import Tuple, List, Dict

def get_pdf_bytes_from_s3(s3_full_path: str, aws_region: str = "us-west-2") -> bytes:
    """
    Download a PDF file from S3 and return its bytes.
    """
    s3 = boto3.client('s3', region_name=aws_region)
    bucket, key = s3_full_path.replace("s3://", "").replace("s3a://", "").replace("s3n://", "").split("/", 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def extract_text_from_pdf(s3_full_path: str, aws_region: str = "us-west-2") -> Tuple[str, List[Dict]]:
    """
    Extract text from a PDF stored in S3 and return structured page-level data.
    """
    try:
        pdf_bytes = get_pdf_bytes_from_s3(s3_full_path, aws_region)
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
    s3_path = "s3://your-bucket-name/path/to/document.pdf"
    status, result = extract_text_from_pdf(s3_path)
    
    if status == "COMPLETED":
        for page in result:
            print(f"Page {page['page_number']}/{page['total_pages']}:")
            print(page['page_text'][:300], "...\n")
    else:
        print("Failed to extract PDF:", result)
