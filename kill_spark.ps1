Get-Process python -ErrorAction SilentlyContinue
Get-Process java -ErrorAction SilentlyContinue
#kill all process for Spark server
Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force