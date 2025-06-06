query_config = {
    "document_number__v" : ['SOP-00409'],
    "status" : ['Approved', 'Effective']
}
#  create vql query using the above query_config for starburst retrieval

secret_name = 'pdm-dl-quality-docs-genai/secrets'

metadata_table_name = "`pdm-pdm-dl-quality-docs-genai-dev`.gvault_test.veeva_qdms_docs_metadata_sample_test"
download_status_table_name = "`pdm-pdm-dl-quality-docs-genai-dev`.gvault_test.download_status_sample_test"
incremental_config_path = "incremental_date/"
number_of_batches = 20

full_load_batch_for_vql = 3000

number_of_partitions_for_download = 120

STATUS_COMPLETED = 'COMPLETED'
STATUS_FAILED = 'FAILED'

# query_config = {
#     "doc_control_unit_name": ["Information Technology"],
#     "type_filters": [
#         {"type": "Governance and Procedures"},
#     ],
#     "status" : ['Approved', 'Effective']
# }

starburst_query_config = {
  "filters": {
    "Kite HQ": [
      {
        "type": "Governance and Procedures",
        "subtypes": ["Procedure", "Policy"]
      },
      { "type": "Business Governance", "subtypes": ["Business Support"] },
      { "type": "Memo", "subtypes": ["Analytical"] },
      { "type": "Protocol", "subtypes": ["Master"] },
      {
        "type": "Report",
        "subtypes": [
          "Material & Manufacturing",
          "Analytical",
          "Facility, Equipment & Instrument"
        ]
      },
      { "type": "Specification", "subtypes": ["Material & Manufacturing"] },
      { "type": "Plan", "subtypes": ["Analytical", "Quality & Compliance"] }
    ],
    # "Kite TCF03": [
    #   { "type": "Governance and Procedures", "subtypes": ["Procedure"] },
    #   { "type": "Business Governance", "subtypes": ["Business Support"] },
    #   { "type": "Memo", "subtypes": ["Analytical"] },
    #   { "type": "Protocol", "subtypes": ["Master"] },
    #   {
    #     "type": "Report",
    #     "subtypes": [
    #       "Material & Manufacturing",
    #       "Analytical",
    #       "Facility, Equipment & Instrument"
    #     ]
    #   },
    #   { "type": "Specification", "subtypes": ["Material & Manufacturing"] }
    # ],
    # "Kite TCF04": [
    #   { "type": "Governance and Procedures", "subtypes": ["Procedure"] },
    #   { "type": "Business Governance", "subtypes": ["Business Support"] },
    #   { "type": "Memo", "subtypes": ["Analytical"] },
    #   { "type": "Protocol", "subtypes": ["Master"] },
    #   {
    #     "type": "Report",
    #     "subtypes": [
    #       "Material & Manufacturing",
    #       "Analytical",
    #       "Facility, Equipment & Instrument"
    #     ]
    #   },
    #   { "type": "Specification", "subtypes": ["Material & Manufacturing"] }
    # ],
    # "Kite TCF05": [
    #   { "type": "Governance and Procedures", "subtypes": ["Procedure"] },
    #   { "type": "Business Governance", "subtypes": ["Business Support"] },
    #   { "type": "Memo", "subtypes": ["Analytical"] },
    #   { "type": "Protocol", "subtypes": ["Master"] },
    #   {
    #     "type": "Report",
    #     "subtypes": [
    #       "Material & Manufacturing",
    #       "Analytical",
    #       "Facility, Equipment & Instrument"
    #     ]
    #   },
    #   { "type": "Specification", "subtypes": ["Material & Manufacturing"] }
    # ],
    # "Kite RDMC": [
    #   { "type": "Governance and Procedures", "subtypes": ["Procedure"] },
    #   { "type": "Business Governance", "subtypes": ["Business Support"] },
    #   { "type": "Memo", "subtypes": ["Analytical"] },
    #   { "type": "Protocol", "subtypes": ["Master"] },
    #   { "type": "Report", "subtypes": ["Material & Manufacturing"] },
    #   { "type": "Specification", "subtypes": ["Material & Manufacturing"] }
    # ],
    # "Kite MVP01": [{ "type": "Report", "subtypes": ["Analytical"] }]
  },
  "status": ["Approved", "Effective"]
}

columns = ['id',
   'name__v',
   'size__v',
   'status__v',
   'title__v',
   'type__v',
   'subtype__v',
   'doc_control_unit__c.name__v',
   'doc_control_unit__c',
   'classification__v',
   'major_version_number__v',
   'minor_version_number__v',
   'lifecycle__v',
   'approved_date__c',
   'final_date__c',
   'effective_date__v',
   'product__v',
   'product__v.name__v',
   'subcategory__c.name__v',
   'document_number__v',
   'owning_function__c.name__v',
   'owning_site__c.name__v',
   'latest_version__v',
   'file_modified_date__v',
   'version_modified_date__v',
   'pages__v']

column_renames = {
    "file": "source_file_path",
    "rendition_type": "rendition_type",
    "id": "document_id",
    "name": "document_name",
    "size": "size",
    "status": "status",
    "title": "document_title",
    "type": "document_type",
    "subtype": "document_subtype",
    "doc_control_unit__c_name": "doc_control_name",
    "doc_control_unit__c": "doc_control_unit_id",
    "classification": "classification",
    "major_version_number": "document_major_version",
    "minor_version_number": "document_minor_version",
    "lifecycle": "lifecycle",
    "effective_date": "effective_date",
    "product": "product",
    "product__v_name": "product_name",
    "document_number": "document_number",
    "owning_function__c_name": "owning_function_name",
    "owning_site__c_name": "owning_site_name",
    "latest_version": "is_latest_version",
    "file_modified_date": "file_modified_date",
    "version_modified_date": "version_modified_date",
    "pages": "pages",
    "dst_path": "dst_path",
    "batch_id": "batch_id",
}

merge_condition = """
    source.document_number = target.document_number AND 
    source.document_minor_version = target.document_minor_version AND 
    source.document_major_version = target.document_major_version
"""

merge_values = {
    "source_file_path": "source.source_file_path",
    "rendition_type": "source.rendition_type",
    "document_id": "source.document_id",
    "document_name": "source.document_name",
    "size": "source.size",
    "status": "source.status",
    "document_title": "source.document_title",
    "document_type": "source.document_type",
    "document_subtype": "source.document_subtype",
    "doc_control_name": "source.doc_control_name",
    "doc_control_unit_id": "source.doc_control_unit_id",
    "classification": "source.classification",
    "document_major_version": "source.document_major_version",
    "document_minor_version": "source.document_minor_version",
    "lifecycle": "source.lifecycle",
    "effective_date": "source.effective_date",
    "product": "source.product",
    "product_name": "source.product_name",
    "document_number": "source.document_number",
    "owning_function_name": "source.owning_function_name",
    "owning_site_name": "source.owning_site_name",
    "is_latest_version": "source.is_latest_version",
    "file_modified_date": "source.file_modified_date",
    "version_modified_date": "source.version_modified_date",
    "pages": "source.pages",
    "dst_path": "source.dst_path",
    "batch_id": "source.batch_id",
    "document_s3_path": "document_s3_path"
}