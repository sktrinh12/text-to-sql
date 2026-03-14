# Welcome to the ELN Text-to-SQL Assistant 🧪

Ask questions about a Electronic Lab Notebook data in plain English — no SQL knowledge required.

---

### What you can ask

**Experiment lookup**
- *Show me all experiments completed by r.shetty in 2016*
- *List the 10 most recent experiments in the AetherGen system*
- *Find experiment ID 138844*

**Cross-scientist analysis**
- *Which scientist has completed the most experiments?*
- *Who countersigned the most entries last year?*

**Status & workflow**
- *How many experiments are still pending countersignature?*
- *Show experiments where the PDF is not yet complete*

**Lab notebook lookups**
- *Find all entries in Book 6 by r.shetty*
- *Show experiments on Protocol ChemELN created in October 2016*

**Free-text search across write-ups**
- *Find experiments that mention methanol or ethanol*
- *Show entries where the product had impurities*
- *Which experiments reference silica gel chromatography?*

---

### Tips

- Be as specific as you like — scientist IDs, date ranges, protocol names all work
- If a query fails the agent will automatically attempt to correct it (up to 3 tries)
- Results are capped at **50 rows** — add *LIMIT* or a filter if you need a narrower set
- Date fields in the notebook are stored as DD/MM/YYYY — you can use natural language like *"created in September 2016"*

---

*Queries run directly against the live PostgreSQL database. Read-only.*
