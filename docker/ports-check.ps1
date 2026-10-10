$ports = @(1883, 8083, 18083, 5432, 5431, 5430, 50051, 50052, 50053, 50054, 8000, 8001, 3001, 9000, 9443)

$inUse = @()

foreach ($port in $ports) {
    $connection = Test-NetConnection -ComputerName localhost -Port $port -WarningAction SilentlyContinue

    if ($connection.TcpTestSucceeded) {
        $inUse += $port
    }
}

if ($inUse.Count -eq 0) {
    Write-Host "ALL OK!"
} else {
    Write-Host "Ports in use:"
    foreach ($port in $inUse) {
        Write-Host "Port $port"
    }
}