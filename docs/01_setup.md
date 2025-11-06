# VVS Platform Setup / Teardown

## 1) Setup

1. **(Optional) Configure plugins**
   Edit `./project_root/launch_config.yaml` to enable the built-in plugins/profiles you want to start.

2. **Start the VVS Platform**

   ```bash
   cd ./project_root
   ./launch_script.sh up -d --build
   ```

   > Allow ~1 minute for all services to become healthy.

3. **(Optional) Run tests**

   * Toggle services in `./project_root/test_config.yaml`
   * Run:

     ```bash
     ./run_tests.sh > tests.txt
     ```

---

## 2) Teardown

Shut down and remove the stack:

```bash
cd ./project_root
./launch_script.sh down
```
