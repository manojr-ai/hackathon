# HealthGPT Care Gap Trust Planner

## Team Titan - [ Manoj Rayalla, Muthu Natarajan, Jatin Balodhi and Arul Anand ]
## Apps & Agents for Good Hackathon MLP* 

**HealthGPT Care Gap Trust Planner:** A Databricks App that helps non-technical healthcare planners identify real medical deserts from messy facility claims, evidence scores, geography, and AI-generated action briefs.

| Category | Description |
|---|---|
| Track | Track 2: Medical Desert Planner |
| Primary Users | Non-technical healthcare planners, NGO coordinators, and analysts |
| Core Decision | Is this a real care gap or a messy-data problem? |
| Main Outputs | Care Gap Score, Evidence Confidence, facility trust signals, review queue, scenarios, and AI action briefs |

## 1. Executive Summary

HealthGPT Care Gap Trust Planner helps healthcare planners, NGO coordinators, and analysts turn messy healthcare facility data into trusted planning decisions. Facility datasets often contain claims such as ICU, maternity, emergency, NICU, dialysis, oncology, trauma, or surgery, but those claims may be incomplete, inconsistent, duplicated, or unsupported by evidence. HealthGPT evaluates each facility claim using transparent evidence rules, aggregates trusted supply across geography, and explains whether a care gap appears real or whether the data needs human review first.

At its core, HealthGPT uses Databricks to transform messy facility records into trusted evidence signals, then uses OpenAI to explain those signals in planner-friendly language.

The application is designed for Track 2: Medical Desert Planner. It helps users answer a practical planning question: Is this area truly missing care capacity, or is the dataset too incomplete to trust?

## 2. Problem

Healthcare planners often receive thousands of facility records with messy fields, missing locations, vague descriptions, and unsupported capability claims. A facility may claim to offer maternity care, emergency care, ICU, NICU, dialysis, oncology, or surgery, but the planner may not know whether that claim is reliable. Counting all claimed facilities can create a false sense of access, while ignoring incomplete records can hide real care options.

HealthGPT addresses this by separating claimed supply from trusted supply. It shows where strong evidence exists, where evidence is partial or weak, where claims are suspicious, and where there is no claim at all.

## 3. What the App Does

The app lets a non-technical planner select a geography and a care capability, then receive a trust-weighted care gap assessment. It calculates a Care Gap Score, an Evidence Confidence level, facility trust signals, a prioritized evidence review queue, and recommended planner actions. It also provides a scenario planner and an AI copilot that explains the results in plain language.

The goal is not to replace human judgment. The goal is to help planners focus their attention on the highest-risk gaps and the highest-leverage records to verify.

## 4. Data Used

HealthGPT combines three types of data. The facility data provides supply-side evidence such as facility name, address, city, state, PIN code, latitude, longitude, description, specialties, procedures, equipment, capacity, number of doctors, claimed capabilities, and source URLs. The PIN code directory provides geography and location context for district, state, latitude, and longitude mapping. The district-level survey data provides need signals such as maternal care, institutional births, vaccination, anemia, nutrition, sanitation, and other health indicators.

In the scoring model, facility evidence is the primary source for trusted supply. Geography determines access and coverage. Survey indicators increase or decrease the severity of the gap but do not prove facility capability by themselves.

## 5. How Trust Scoring Works

For each facility and capability, the app calculates a Facility Capability Evidence Score from 0 to 100. The score considers whether the capability is explicitly claimed and whether it is supported by facility description, specialties, procedures, equipment, capacity, doctors, and source URLs. Penalties are applied for contradictions, missing location, sparse records, duplicate records, or unsupported claims.

The score is converted into a trust signal: Strong Evidence, Partial Evidence, Weak Evidence, Suspicious, or No Claim. Suspicious is used when the record contains a capability claim but the supporting evidence is missing or contradictory.

## 6. Care Gap Score

For each selected geography and capability, the app calculates a Care Gap Score using four inputs: trust-weighted supply shortage, geographic access risk, survey-based need burden, and data quality or evidence uncertainty. Strong facilities count fully toward supply, partial facilities count partially, weak facilities count minimally, and suspicious facilities do not count as trusted supply until verified.

This prevents the app from over-counting questionable facility claims and helps planners distinguish real medical deserts from data-poor regions.

## 7. Key Features and Screens

Overview: Shows the selected geography and capability, the Care Gap Score, Evidence Confidence, trusted facility counts, top indicators, and recommended actions.

Care Map: Displays facilities on a map using trust signals: strong, partial, weak, suspicious, or no claim. This helps planners see where trusted care exists and where coverage gaps may be real.

Facilities: Shows the evidence behind each facility's trust signal, including capability, description, specialties, procedures, equipment, source URLs, and location quality.

Evidence Review: Prioritizes high-leverage records for manual verification, especially weak or suspicious claims in high-gap areas.

Scenario Planner: Lets planners test what happens if suspicious claims are rejected, weak claims are verified, new capacity is added, or missing location/source data is corrected. The app recalculates the gap score and confidence.

Ask HealthGPT: Provides evidence-grounded Q&A and generates planner-ready explanations, review plans, and action briefs.

Architecture / Trust: Shows the data lineage from raw facility records through Databricks cleaning, scoring, aggregation, OpenAI explanation, and saved planner decisions.

## 8. Role of Databricks and OpenAI

Databricks performs the trusted data work: ingesting raw records, cleaning and standardizing data, extracting facility capability evidence, calculating trust scores, aggregating supply by geography, joining survey need signals, and producing final care gap outputs.

OpenAI is used after these structured signals are created. It explains why an area is flagged, summarizes evidence, answers planner questions, and generates action briefs. OpenAI does not independently invent the scores. It receives Databricks-calculated scores, facility evidence, confidence levels, review priorities, and scenario outputs.

## 9. User Workflow

Select a geography such as state, district, city, or PIN code.
Select a care capability such as maternity, emergency, ICU, NICU, dialysis, oncology, or trauma.
Review the care gap overview and confidence level.
Open the map to see facility trust signals across geography.
Inspect facility evidence details for strong, partial, weak, suspicious, and no-claim records.
Use the evidence review queue to identify which records require manual verification first.
Run scenarios to understand how verification or capacity changes affect the gap score and confidence.
Ask HealthGPT for a plain-language explanation, review plan, or action brief.
Save planner decisions, notes, overrides, shortlists, and scenarios for follow-up.

## 10. Why This Matters

Bad facility data can lead to bad planning decisions. A planner may think a district has enough emergency or maternity capacity because many facilities claim those services, but the evidence may be weak or suspicious. HealthGPT makes that uncertainty visible. It helps planners avoid false confidence, prioritize verification work, and focus resources on areas where care gaps are most likely to be real.

The app is especially useful for healthcare planning, NGO program design, field verification, referral network planning, and data-quality improvement.

## 11. Project Impact

HealthGPT helps decision-makers move from messy records to defensible planning decisions. It shortens the path from raw data to action by combining transparent scoring, geospatial aggregation, evidence review, scenario planning, and AI explanation. The result is a more trustworthy way to identify medical deserts, validate facility capability claims, and decide what to investigate or fund next.

The architecture is designed to make the recommendations explainable and auditable, not just AI-generated.

## 12. Architecture

HealthGPT Care Gap Trust Planner is built as a Databricks-centered decision-support application. The architecture separates structured evidence scoring from AI-generated explanation so planners can trust how each recommendation was produced.

The system uses three data layers:

1. **Facility Data Layer**
   Raw facility records include facility name, address, city, state, PIN code, latitude, longitude, description, specialties, procedures, equipment, capability claims, capacity, doctors, and source URLs. These fields are treated as claims to evaluate, not as guaranteed truth.

2. **Geography Layer**
   PIN code and district reference data are used to map facilities to geographies, fill missing location context, and aggregate trusted facility supply by district, city, or PIN code.

3. **Health Need Layer**
   District-level survey indicators such as maternal care, institutional births, anemia, vaccination, nutrition, and other public health signals are used to prioritize which care gaps are most urgent.

The data pipeline follows this flow:

Raw Facility Records
→ Databricks Bronze Tables
→ Cleaned Silver Records
→ Gold Facility Capability Evidence Scores
→ Trust-Weighted Geographic Aggregation
→ Care Gap Score and Evidence Confidence
→ OpenAI Explanation Layer
→ Planner UI and Saved Decisions

Databricks performs the core calculations, including data cleaning, capability extraction, evidence scoring, trust signal generation, geographic aggregation, care gap scoring, review queue prioritization, and scenario recalculation.

OpenAI is used only after the structured scores are created. It explains why a care gap was detected, summarizes supporting evidence, answers planner questions, and generates action briefs in plain language.

This architecture ensures that HealthGPT is not simply generating recommendations from a prompt. The app first creates transparent, auditable evidence signals in Databricks, then uses AI to make those signals understandable and actionable for non-technical healthcare planners.


## 12. Closing Statement

HealthGPT Care Gap Trust Planner helps planners separate real care deserts from messy-data problems, so they can make faster, safer, and more defensible healthcare planning decisions.

## Feature and Functionality Summary

| Screen / Feature | Functionality |
|---|---|
| Overview | Summarizes care gap score, confidence, trusted facility counts, top indicators, and recommended actions. |
| Care Map | Maps strong, partial, weak, suspicious, and no-claim facilities to reveal trusted coverage gaps. |
| Facilities | Explains the evidence behind each facility-capability trust signal. |
| Evidence Review | Prioritizes high-impact records for manual verification. |
| Scenario Planner | Recalculates gap score and confidence after simulated verification, rejection, or capacity changes. |
| Ask HealthGPT | Generates evidence-grounded explanations, review plans, and action briefs. |
| Architecture / Trust | Shows lineage, scoring logic, and persisted planner decisions. |




**Team Titan**



Note:
*Minimum Lovable Product
