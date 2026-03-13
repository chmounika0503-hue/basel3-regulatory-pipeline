import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.units import inch
from datetime import datetime
import os

# --- Config ---
DB_URL = "host=localhost port=5432 dbname=basel3 user=bankuser password=bankpass"
OUTPUT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Fetch data ---
conn = psycopg2.connect(DB_URL)
df = pd.read_sql("SELECT * FROM benchmark_comparison ORDER BY bank_id", conn)
conn.close()

# Convert to percentages
df["car_pct"] = (df["car"] * 100).round(2)
df["lcr_pct"] = (df["lcr"] * 100).round(2)
df["npl_pct"] = (df["npl"] * 100).round(2)
df["benchmark_car_pct"] = (df["benchmark_car"] * 100).round(2)
df["benchmark_lcr_pct"] = (df["benchmark_lcr"] * 100).round(2)
df["benchmark_npl_pct"] = (df["benchmark_npl"] * 100).round(2)

# --- Chart helpers ---
def make_bar_chart(filename, title, banks, values, benchmark, threshold, ylabel, color):
    fig, ax = plt.subplots(figsize=(7, 3.5))
    bar_colors = [color if v >= threshold else "crimson" for v in values]
    bars = ax.bar(banks, values, color=bar_colors, edgecolor="white", width=0.5)
    ax.axhline(y=threshold, color="red", linestyle="--", linewidth=1.5, label=f"Min threshold ({threshold}%)")
    ax.axhline(y=benchmark, color="orange", linestyle="--", linewidth=1.5, label=f"FDIC benchmark ({benchmark}%)")
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3, f"{val}%", ha="center", fontsize=9)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_ylabel(ylabel)
    ax.legend(fontsize=8)
    ax.set_ylim(0, max(values + [benchmark, threshold]) * 1.3)
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()

chart_car = f"{OUTPUT_DIR}/chart_car.png"
chart_lcr = f"{OUTPUT_DIR}/chart_lcr.png"
chart_npl = f"{OUTPUT_DIR}/chart_npl.png"

make_bar_chart(chart_car, "Capital Adequacy Ratio (CAR)", df["bank_id"].tolist(),
               df["car_pct"].tolist(), df["benchmark_car_pct"].iloc[0], 8.0, "CAR (%)", "#4C72B0")
make_bar_chart(chart_lcr, "Liquidity Coverage Ratio (LCR)", df["bank_id"].tolist(),
               df["lcr_pct"].tolist(), df["benchmark_lcr_pct"].iloc[0], 100.0, "LCR (%)", "#55A868")
make_bar_chart(chart_npl, "Non-Performing Loan Ratio (NPL)", df["bank_id"].tolist(),
               df["npl_pct"].tolist(), df["benchmark_npl_pct"].iloc[0], 5.0, "NPL (%)", "#C44E52")

# --- Build PDF ---
report_date = datetime.now().strftime("%B %Y")
filename = f"{OUTPUT_DIR}/basel3_regulatory_report_{datetime.now().strftime('%Y%m')}.pdf"
doc = SimpleDocTemplate(filename, pagesize=letter, topMargin=0.75*inch, bottomMargin=0.75*inch)
styles = getSampleStyleSheet()
story = []

# Title
title_style = ParagraphStyle("title", parent=styles["Title"], fontSize=18, spaceAfter=6)
story.append(Paragraph("Basel III Regulatory Compliance Report", title_style))
story.append(Paragraph(f"Reporting Period: {report_date}", styles["Normal"]))
story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles["Normal"]))
story.append(Spacer(1, 0.2*inch))

# Executive Summary
story.append(Paragraph("Executive Summary", styles["Heading2"]))
passing_car = (df["car"] >= 0.08).sum()
passing_lcr = (df["lcr"] >= 1.0).sum()
total = len(df)
summary = f"""
This report presents Basel III regulatory metrics for {total} banks as of {report_date}.
<br/><br/>
<b>CAR:</b> {passing_car}/{total} banks meet the minimum 8% Capital Adequacy Ratio requirement.
<br/>
<b>LCR:</b> {passing_lcr}/{total} banks meet the minimum 100% Liquidity Coverage Ratio requirement.
<br/>
<b>Data Source:</b> Simulated balance sheet data benchmarked against FDIC industry averages.
"""
story.append(Paragraph(summary, styles["Normal"]))
story.append(Spacer(1, 0.2*inch))

# Metrics Table
story.append(Paragraph("Bank-by-Bank Metrics", styles["Heading2"]))
table_data = [["Bank", "CAR (%)", "CAR Status", "LCR (%)", "LCR Status", "NPL (%)"]]
for _, row in df.iterrows():
    table_data.append([
        row["bank_id"],
        f"{row['car_pct']}%",
        "PASS" if row["car"] >= 0.08 else "FAIL",
        f"{row['lcr_pct']}%",
        "PASS" if row["lcr"] >= 1.0 else "FAIL",
        f"{row['npl_pct']}%",
    ])

t = Table(table_data, colWidths=[1.0*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.1*inch, 1.1*inch])
t.setStyle(TableStyle([
    ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#2C3E50")),
    ("TEXTCOLOR", (0,0), (-1,0), colors.white),
    ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
    ("ALIGN", (0,0), (-1,-1), "CENTER"),
    ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
    ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F2F3F4")]),
]))

# Color PASS/FAIL cells
for i, row in enumerate(df.itertuples(), start=1):
    car_color = colors.HexColor("#27AE60") if row.car >= 0.08 else colors.HexColor("#E74C3C")
    lcr_color = colors.HexColor("#27AE60") if row.lcr >= 1.0 else colors.HexColor("#E74C3C")
    t.setStyle(TableStyle([
        ("BACKGROUND", (2, i), (2, i), car_color),
        ("TEXTCOLOR", (2, i), (2, i), colors.white),
        ("BACKGROUND", (4, i), (4, i), lcr_color),
        ("TEXTCOLOR", (4, i), (4, i), colors.white),
    ]))

story.append(t)
story.append(Spacer(1, 0.3*inch))

# Charts
story.append(Paragraph("CAR Analysis", styles["Heading2"]))
story.append(Image(chart_car, width=6.5*inch, height=3.2*inch))
story.append(Spacer(1, 0.2*inch))

story.append(Paragraph("LCR Analysis", styles["Heading2"]))
story.append(Image(chart_lcr, width=6.5*inch, height=3.2*inch))
story.append(Spacer(1, 0.2*inch))

story.append(Paragraph("NPL Analysis", styles["Heading2"]))
story.append(Image(chart_npl, width=6.5*inch, height=3.2*inch))

# Basel III Thresholds Reference
story.append(Spacer(1, 0.3*inch))
story.append(Paragraph("Basel III Minimum Requirements", styles["Heading2"]))
ref_data = [
    ["Metric", "Minimum Requirement", "FDIC Industry Benchmark"],
    ["CAR", "8%", f"{df['benchmark_car_pct'].iloc[0]}%"],
    ["LCR", "100%", f"{df['benchmark_lcr_pct'].iloc[0]}%"],
    ["NPL", "< 5% (guideline)", f"{df['benchmark_npl_pct'].iloc[0]}%"],
]
ref_table = Table(ref_data, colWidths=[2*inch, 2*inch, 2.5*inch])
ref_table.setStyle(TableStyle([
    ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#2C3E50")),
    ("TEXTCOLOR", (0,0), (-1,0), colors.white),
    ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
    ("ALIGN", (0,0), (-1,-1), "CENTER"),
    ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
    ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#F2F3F4")]),
]))
story.append(ref_table)

doc.build(story)
print(f"Report generated: {filename}")
