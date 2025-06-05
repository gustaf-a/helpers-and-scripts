Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public class BluetoothAPIs
{
    [DllImport("BluetoothAPIs.dll", SetLastError = true)]
    public static extern int BluetoothFindFirstRadio(ref Guid pbtfrp, out IntPtr phRadio);

    [DllImport("BluetoothAPIs.dll", SetLastError = true)]
    public static extern int BluetoothEnableDiscovery(IntPtr hRadio, bool fEnabled);

    [DllImport("BluetoothAPIs.dll", SetLastError = true)]
    public static extern int BluetoothEnableIncomingConnections(IntPtr hRadio, bool fEnabled);

    [DllImport("BluetoothAPIs.dll", SetLastError = true)]
    public static extern int BluetoothIsConnectable(IntPtr hRadio);
}
'@

$guid = New-Object Guid
$hRadio = [System.IntPtr]::Zero
$result = [BluetoothAPIs]::BluetoothFindFirstRadio([ref]$guid, [ref]$hRadio)

# Check the result
if ($result -ne 0) {
    Write-Host "0 Press any key to continue..."
    $x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

    throw "Failed to find a Bluetooth radio"
}

# Now you can use $hRadio with BluetoothIsConnectable
$btStatus = [BluetoothAPIs]::BluetoothIsConnectable($hRadio)

if ($btStatus -eq 0) {
    $result = [BluetoothAPIs]::BluetoothEnableDiscovery($hRadio, $true)
    if ($result -ne 0) {
         Write-Host "12 Press any key to continue..."
    $x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

        throw "Failed to enable Bluetooth discovery"
    }
    $result = [BluetoothAPIs]::BluetoothEnableIncomingConnections($hRadio, $true)
    if ($result -ne 0) {
        Write-Host "13 Press any key to continue..."
    $x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
        throw "Failed to enable Bluetooth incoming connections"
    }
}


Write-Host "1 Press any key to continue..."
$x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

$connectedDevices = Get-ConnectedDevice
foreach ($connectedDevice in $connectedDevices) {
    if ($connectedDevice.Name.Trim() -ne $DeviceNames[0].Trim()) {
        $connectedDevice | Disconnect-Device
        Write-Host "Disconnected from $($connectedDevice.Name)"
    }
}

Write-Host "2 Press any key to continue..."
$x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

foreach ($deviceName in $DeviceNames) {
    $pairedDevices = Get-PairedDevice
    $device = $pairedDevices | Where-Object { $_.Name.Trim() -like "*$($deviceName.Trim())*" }

    if ($device -ne $null) {
        $device | Connect-Device
        Write-Host "Connected to $deviceName"
        break
    } else {
        Write-Host "$deviceName not found"
    }

    Write-Host "3 Press any key to continue..."
    $x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
}

Write-Host "Press any key to exit..."
$x = $host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
