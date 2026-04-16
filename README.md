
# Automatic Business Finder - Shareable Versions

This folder includes:

- `business_finder_core.py` → reusable BBB scraping + Excel logic
- `streamlit_app.py` → browser web app
- `automatic_business_finder_colab.ipynb` → Google Colab notebook with widgets
- `requirements.txt` → packages for Streamlit deployment

## Streamlit
Run locally:

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Colab
Upload the notebook to Colab, upload `cities.csv` when prompted, then run all cells.
