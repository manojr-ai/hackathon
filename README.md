## Databricks Apps & Agents for Good Hackathon 2026 - Team Titan
## Team Members - Manoj Rayalla, Muthu Natarajan, Jatin Balodhi and Arul Anand
## Executive Summary

**HealthGPT Care Gap Trust Planner** is a Databricks App that helps non-technical healthcare planners, NGO coordinators, and analysts turn messy healthcare facility data into decisions they can trust.

The project is designed for **Track 2: Medical Desert Planner**. It identifies where care gaps are likely to exist, how severe those gaps are, and how confident we are that the gap is real versus caused by incomplete or unreliable data.

Healthcare facility datasets often contain noisy claims such as “ICU,” “maternity,” “emergency,” “NICU,” “dialysis,” or “oncology,” but those claims are not always supported by descriptions, specialties, procedures, equipment, doctors, source URLs, or geographic coverage. HealthGPT solves this by scoring each facility capability claim using transparent evidence rules, converting those scores into trust signals, and aggregating them across geography to detect likely medical deserts.

The app combines three data layers:

1. **Facility Evidence Layer**
   Uses facility name, location, description, specialties, procedures, equipment, capacity, doctors, capability fields, and source URLs to calculate a facility-level trust signal.

2. **Geographic Access Layer**
   Uses PIN code, district, state, latitude, and longitude data to understand where trusted facilities are located and where access gaps may exist.

3. **Health Need Layer**
   Uses district-level survey indicators such as maternal care, institutional births, vaccination, anemia, nutrition, and other public health signals to prioritize gaps that matter most.

For each selected geography and capability, the app calculates:

* **Care Gap Score**
* **Evidence Confidence**
* **Trust-weighted facility supply**
* **Strong, partial, weak, suspicious, and no-claim facility counts**
* **High-priority records for manual review**
* **Recommended planner actions**
* **Scenario impact if claims are verified, rejected, or new capacity is added**

The core product workflow is simple:

1. A planner selects a geography and care capability.
2. The app displays a trust-weighted care gap overview.
3. The care map shows where strong, partial, weak, suspicious, and no-claim facilities are located.
4. The facility evidence screen explains why each facility received its trust signal.
5. The evidence review queue prioritizes which facility records should be manually verified first.
6. The scenario planner shows how verification or capacity changes affect the gap score and confidence.
7. Ask HealthGPT generates evidence-grounded explanations, review plans, and action briefs.

HealthGPT is intentionally not just a chatbot. Databricks performs the data cleaning, evidence scoring, geographic aggregation, and care gap calculations. OpenAI is used only after the structured signals are created, helping explain the results in plain language and generate planner-ready briefs.

The goal is to help decision-makers answer a critical planning question:

> “Is this area truly missing care capacity, or is the dataset too incomplete to trust?”

By separating real care gaps from data-quality problems, HealthGPT helps healthcare planners make faster, clearer, and more defensible decisions.
