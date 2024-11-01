#!/usr/bin/env python3

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle

def generate_report(filename, data):
    c = canvas.Canvas(filename, pagesize=A4)
    width, height = A4
    y_position = height - 40  # Starting y-position for content
    
    # Info Section
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y_position, "Info")
    y_position -= 20
    
    c.setFont("Helvetica", 10)
    for section, items in data["info"].items():
        c.drawString(40, y_position, f"## {section}")
        y_position -= 15
        for label, value in items.items():
            c.drawString(60, y_position, f"**{label}:** {value}")
            y_position -= 15
        y_position -= 10

    # Definitions Section
    y_position -= 20
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y_position, "Definitions")
    y_position -= 20

    table_data = data["definitions"]["Table 1"]
    table = Table(table_data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ]))
    table.wrapOn(c, width, y_position)
    table.drawOn(c, 40, y_position - (len(table_data) * 15 + 20))

    # Files Changed Section
    y_position -= (len(table_data) * 15 + 60)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, y_position, "Files changed")
    y_position -= 20
    c.setFont("Helvetica", 10)
    for category, files in data["files_changed"].items():
        c.drawString(40, y_position, f"## {category}")
        y_position -= 15
        c.drawString(60, y_position, files)
        y_position -= 10

    # Save the PDF
    c.save()

# TODO
report_data = {
    "info": {
        "Initial environment": {
            "Environment": "[${STAGING_BUCKET_TYPE}]",
            "Bucket": "$staging_bucket",
            # Add more fields as needed
        },
        "Target environment": {
            "Environment": "[curated]",
            "Bucket": "gs://asap-curated-data-${team}",
            # Add more fields as needed
        }
    },
    "definitions": {
        "Table 1": [
            ["Term", "Definition"],
            ["New files", "Set of new files (i.e. they didn’t exist in previous runs/workflow versions)."],
            ["Modified files", "Set of files that have different checksums."],
            # Add more rows as needed
        ]
    },
    "files_changed": {
        "New (i.e. only in staging)": "${new_files_table}",
        "Modified": "${mod_files_table}",
        "Deleted (i.e. only in prod)": "${deleted_files_table}"
    }
    # Add more sections as needed
}

generate_report("report.pdf", report_data)
