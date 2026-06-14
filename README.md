# VVS Platform 

Vector Virtual Screening (VVS) Platform, implementing the VVS algorithm from the paper [Efficient Search of Ultra-Large Synthesis On-Demand Libraries with Chemical Language Models](https://www.biorxiv.org/content/10.1101/2025.09.04.674350v1).

Code subject to change during peer review process.

See `./docs` folder for setup, software demo, and other instructions 

## 1. System Requirements

### Software dependencies
The platform runs as a Docker Compose stack; all internal service and Python dependencies are pinned within the provided container images (see individual `Dockerfile`s and `requirements.txt` files in each service directory). Host requirements are:

- **Operating system:** Linux (tested on Ubuntu 20.04 LTS)
- **Docker Engine:** ≥ 24.0 (tested on 24.0.7)
- **Docker Compose:** ≥ 2.29 (tested on 2.29.2)
- **NVIDIA Driver:** ≥ 550 (tested on 550.90.07)
- **NVIDIA Container Toolkit:** ≥ 1.15 (required for GPU passthrough to the Triton inference container)

### Hardware requirements
- Optional: an NVIDIA GPU is required to use the Triton Plugin and TEI Plugin
- **Tested on:** NVIDIA RTX 3090 (24 GB), A100
- Recommended ≥ 32 GB RAM, ≥ 100 GB disc space for images and vector databases (tested on 126 GB RAM, 1 TB disk)

### Tested configurations
- Ubuntu 22.04, Docker 24.0.7, Compose 2.29.2, NVIDIA driver 550.90.07, RTX 3090
- Ubuntu 22.04, Docker 24.0.7, Compose 2.29.2, NVIDIA driver 550.90.07, A100

## 2. Installation Guide

### Instructions
```bash
git clone https://github.com/DarkMatterAI/VVS_platform/tree/main
cd VVS_platform/project_root

# (Optional) edit ./project_root/launch_config.yaml to configure plugins 

./launch_script.sh up -d --build
```

Verify the services are running:

```bash
docker compose ps
```

The REST API will be available at `http://localhost:3000` (configurable via `NGINX_HTTP_PORT`). The Dagster dashboard will be available at `http://localhost:3000/dagster/` (configurable via `NGINX_HTTP_PORT`).

### Typical install time
On a normal desktop with a broadband connection, building and pulling all images takes approximately 5-10 minutes (dominated by image downloads on first run).

## 3. Demo

### Instructions
A worked example is provided in `VVS_platform/docs/04_demo.md`.

The demo runs a building block space search over a toy dataset of 100 Enamine building blocks, optimizing the QED score. See `VVS_platform/docs/04_demo.md` for a step-by-step walkthrough.

### Expected output
The demo returns a list of assembled, synthesizable molecules ranked by QED score, retrieved from the toy building block library. An example output is provided at `VVS_platform/docs/demo_files/demo_results.json`.

### Expected run time
The demo completes in approximately 5-10 minutes on a normal desktop with a single GPU.

## 4. Instructions for Use

### Running on your own data
1. Prepare a CSV of building blocks following the schema of `demo_files/demo_bbs.csv`.
2. Upload it and create a Qdrant data source (see `VVS_platform/docs/04_demo.md`, "Setup: Qdrant Data Source").
3. Prepare a CSV of query SMILES following the schema of `demo_files/demo_smiles.csv`.
4. Configure and submit a search job (see `VVS_platform/docs/04_demo.md`, "Create Job Config" and "Execute Job on Backend").


## Bibtex

```
@UNPUBLISHED{Heyer2025-vx,
  title     = {"Efficient search of ultra-large synthesis on-demand libraries
               with chemical language models"},
  author    = {"Heyer, Karl and Yang, David and Diaz, Daniel J"},
  journal   = {"bioRxiv"},
  month     = {sep},
  year      = {2025},
  url       = {https://www.biorxiv.org/content/10.1101/2025.09.04.674350v1}
}
```

## License
For non-commercial academic use, this project is licensed under [the 2-clause BSD License](https://opensource.org/license/bsd-2-clause). 
For commercial use, please contact [Karl Heyer](mailto:karl@darkmatterai.xyz).