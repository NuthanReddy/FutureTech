[Setup]
AppName=Storage Visualizer
AppVersion=1.0
AppPublisher=Nuthan
DefaultDirName={autopf}\StorageVisualizer
DefaultGroupName=Storage Visualizer
OutputBaseFilename=StorageVisualizerSetup
Compression=lzma2
SolidCompression=yes
PrivilegesRequired=admin
SetupIconFile=icon.ico
UninstallDisplayIcon={app}\StorageVisualizer.exe

[Files]
Source: "C:\Users\ngantla\PycharmProjects\Nuthan\dist\StorageVisualizer\*"; DestDir: "{app}"; Flags: recursesubdirs
Source: "icon.ico"; DestDir: "{app}"

[Icons]
Name: "{group}\Storage Visualizer"; Filename: "{app}\StorageVisualizer.exe"; IconFilename: "{app}\icon.ico"
Name: "{commondesktop}\Storage Visualizer"; Filename: "{app}\StorageVisualizer.exe"; IconFilename: "{app}\icon.ico"

[Run]
Filename: "{app}\StorageVisualizer.exe"; Description: "Launch Storage Visualizer"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}"
