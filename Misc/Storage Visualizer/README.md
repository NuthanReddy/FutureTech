# Storage Visualizer

A small desktop app for exploring disk usage with a one-level-at-a-time donut chart.

## Features

- scans one folder level at a time and loads deeper levels on drill-down
- draws one donut ring for immediate children of the current folder
- drills into a folder when you click its chart segment
- shows breadcrumbs, current-folder summary, and child details
- optionally groups small siblings into `Others`
- keeps small items separate when their combined share is below 5%

## Run

```powershell
uv run python "Misc/Storage Visualizer/storage_graph.py"
uv run python "Misc/Storage Visualizer/storage_graph.py" "C:\Users\ngantla\Downloads"
uv run python "Misc/Storage Visualizer/storage_graph.py" --group-small
```

## Build Executable

You can package the app into a standalone `.exe` (Windows) or native binary (macOS/Linux) using [PyInstaller](https://pyinstaller.org/):

```bash
# Install PyInstaller (use pip, not uv, to avoid pulling in incompatible project deps)
pip install pyinstaller

# Build a single-file executable
pyinstaller --onefile --windowed --name "StorageVisualizer" "Misc/Storage Visualizer/storage_graph.py"
```

The output binary will be in the `dist/` folder.

> **⚠️ Don't use `uv run pyinstaller`** — the monorepo has heavy deps (tensorflow, spleeter, etc.) that may fail to resolve on Windows. Since the visualizer only uses the standard library, plain `pip install pyinstaller && pyinstaller ...` in a clean venv is the simplest path.

### Options

| Flag | Purpose |
|------|---------|
| `--onefile` | Bundle everything into a single `.exe` |
| `--windowed` | Suppress the console window (GUI app) |
| `--icon=icon.ico` | Set a custom icon (optional) |
| `--name "StorageVisualizer"` | Name the output binary |

### Cross-platform notes

- **Windows**: Produces `dist/StorageVisualizer.exe`. Requires no Python on the target machine.
- **macOS**: Produces `dist/StorageVisualizer` (or use `--osx-bundle-identifier` for a `.app` bundle). The dark title bar feature uses Windows-only APIs and is skipped on macOS.
- **Linux**: Produces `dist/StorageVisualizer`. Requires Tkinter to be available in the Python used for building (`sudo apt install python3-tk` on Debian/Ubuntu).

Build must be done **on the target OS** — PyInstaller does not cross-compile.

### Recommended build steps (using .venv)

```powershell
# From the project root — uses the existing .venv managed by uv
.venv\Scripts\python.exe -m ensurepip
.venv\Scripts\python.exe -m pip install pyinstaller
.venv\Scripts\pyinstaller.exe -y --onedir --windowed --icon "Misc\Storage Visualizer\icon.ico" --name "StorageVisualizer" "Misc\Storage Visualizer\storage_graph.py"
```

### Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `uv run pyinstaller` fails with *tensorflow-io-gcs-filesystem* resolve error | Monorepo deps include TensorFlow which has no Windows wheel for that version | Don't use `uv run`; build in a clean venv with only PyInstaller |
| **"Bad Image" / `ucrtbase.dll` error** when running the `.exe` | Built with Microsoft Store Python which bundles incompatible system DLLs | Uninstall Store Python; use the standard installer from [python.org](https://www.python.org/downloads/) |
| **"Application Control policy has blocked this file"** | Corporate AppLocker/WDAC blocks unsigned DLLs extracted to `%TEMP%` by `--onefile` mode | Use `--onedir` instead (see below), or run from source with `python storage_graph.py` |
| `.exe` blocked by antivirus | PyInstaller one-file executables are commonly flagged | Sign the binary, or use `--onedir` instead of `--onefile` |

### Using `--onedir` on managed machines

If your machine has AppLocker or WDAC policies, `--onefile` will fail because it
extracts DLLs to `%TEMP%` at runtime. Use `--onedir` instead — it creates a folder
with the exe and all dependencies side-by-side (no temp extraction):

```powershell
pyinstaller --onedir --windowed --name "StorageVisualizer" "Misc/Storage Visualizer/storage_graph.py"
```

This produces `dist/StorageVisualizer/StorageVisualizer.exe`. Distribute the entire
`StorageVisualizer/` folder. You can zip it for sharing.

## Package as a Windows Installer

On managed machines (AppLocker/WDAC), even `--onedir` may be blocked when run from untrusted locations. A proper installer that writes to `C:\Program Files` solves this because that path is trusted by default.

### Step 1: Build with `--onedir`

```powershell
python -m venv .build-venv
.build-venv\Scripts\activate
pip install pyinstaller
pyinstaller --onedir --windowed --name "StorageVisualizer" "Misc/Storage Visualizer/storage_graph.py"
deactivate
```

This creates `dist/StorageVisualizer/` with the exe and all dependencies.

### Step 2: Create an installer with Inno Setup

1. Download and install [Inno Setup](https://jrsoftware.org/isinfo.php) (free)
2. Save the following as `installer.iss` in the project root:

```ini
[Setup]
AppName=Storage Visualizer
AppVersion=1.0
DefaultDirName={autopf}\StorageVisualizer
DefaultGroupName=Storage Visualizer
OutputBaseFilename=StorageVisualizerSetup
Compression=lzma2
SolidCompression=yes
PrivilegesRequired=admin

[Files]
Source: "..\..\dist\StorageVisualizer\*"; DestDir: "{app}"; Flags: recursesubdirs

[Icons]
Name: "{group}\Storage Visualizer"; Filename: "{app}\StorageVisualizer.exe"
Name: "{commondesktop}\Storage Visualizer"; Filename: "{app}\StorageVisualizer.exe"

[Run]
Filename: "{app}\StorageVisualizer.exe"; Description: "Launch Storage Visualizer"; Flags: nowait postinstall skipifsilent
```

3. Open the `.iss` file in Inno Setup Compiler and click **Build → Compile**
4. The installer `StorageVisualizerSetup.exe` will appear in the `Output/` folder

### Alternative: MSIX (no third-party tools)

If you have the Windows SDK, you can use `makeappx` to create an MSIX package. This is more complex but produces a Store-ready package.

### Alternative: just zip it

For quick sharing without admin rights, zip the `dist/StorageVisualizer/` folder and have users extract it to a non-temp location (e.g., `C:\Tools\`). AppLocker typically allows execution from user-owned directories outside `%TEMP%`.

## Notes

- The app uses Tkinter from the Python standard library, so no extra package is required.
- `Back` returns to the previous drill-down location.
- `Up` moves to the current folder's parent within the scanned tree.
