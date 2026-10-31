# AI Prompt History

This document satisfies **Guideline 03 (USE LLMs TRANSPARENTLY)** and **FAQ Items 4 & 6** of the YottaFlex AI Candidate Project Submission Guidelines.

---

## Prompt 1: Initial Problem Statement & Implementation Requirements
**Prompt Submitted:**
```text
Associate AI Forward Deployed Engineers · AI Data Forward Deployed Engineer
Problem statement:
Process change events for inserts, updates, and deletes into a current-state table while preserving an auditable history. Handle duplicate and out-of-order events. Deliver a working Data Engineering solution within 72 hours.
The solution should be runnable locally, expose failures and data-quality issues, and produce measurable outputs.
Target level: 0–3 years of experience. Prioritize correctness, clear design, maintainability, and evidence of testing over unnecessary complexity.
1. Define source and target schemas, data types, keys, and assumptions.
2. Generate or provide a small reproducible sample dataset and a larger scalable dataset generator.
3. Implement ingestion, validation, transformation, and target publishing.
4. Make reruns safe; record run status, row counts, rejects, and errors.
5. Include automated tests and demonstrate expected results with sample queries or reports.
Entities and Relationships:
Identify source entities, keys, grain, and business rules. Document nullability, uniqueness, referential integrity.
Keep rejected records traceable. Preserve change history instead of silently overwriting it.
Required Edge Cases:
Malformed input; missing required field; duplicate key/event; unexpected schema change; empty batch; partial failure and retry; late-arriving/out-of-order data; invalid reference key; rerun of same input.
```

---

## Prompt 2: Live Pipeline Execution Request
**Prompt Submitted:**
```text
run the project
```

---

## Prompt 3: Output Artifacts & Result Inspection
**Prompt Submitted:**
```text
is it the output or where can i see the output results
```

---

## Prompt 4: Step-by-Step Running Guide
**Prompt Submitted:**
```text
explain step by step where can i run this project vs code or command prompt give me the steps
```

---

## Prompt 5: Technical Details, Languages, CDC Operations & Re-Execution
**Prompt Submitted:**
```text
in this project what are the languages and database used and what is the purpose of those and what are the pipelines and how it works everything what is mentioned in problem statement is that completed. in the problem statement there are insert,delete,update what is those and did you implement those if you implement show me and run the output once again
```

---

## Prompt 6: Submission Guidelines & PDF Compliance Audit
**Prompt Submitted:**
```text
once analyze these pdfs and check once eveerything is implemented or not
[PDF Attachments: Candidate Project Submission Guidelines & Candidate Project FAQ]
```

---

## Prompt 7: GitHub Push & Submission Verification
**Prompt Submitted:**
```text
this is my github repo push this project into this repo and readmefile also with step by step detailed explaination of this project how it is there in the pdf
[Attachment: Screenshot of GitHub repository https://github.com/Malleswarikomara/CDC_DataPipeline-Project.git]
```
