# SwissData

SwissData is a set of tools for working with timing data from races and training runs.

## What it does

- receives race data from the timing server
- shows current results in a web interface
- groups athletes by category
- shows athlete history by distance
- shows best results and all splits
- generates protocols and test data
- includes desktop tools and simulators for testing

## How to use

### 1. Run the stats website

Start `SwissStatsService.exe` and open the local web page it provides.

### 2. Run the server

Start `QuantumServerQt.exe` to receive timing data and send results to the stats website.

### 3. Open the web interface

In the browser you can:

- select a category
- pick an athlete
- view all runs for that athlete
- see the best result and all splits
- open the category top list

### 4. Generate protocols

Use the client tools to build protocols from saved race data.

## Included tools

- `QuantumServerQt.exe` - main timing server
- `SwissStatsService.exe` - web statistics service
- `QuantumClient.exe` - client/protocol tool
- `QuantumSimulator.exe` - simulator

## Notes

- The project is designed for live race data and offline test data.
- If you have saved results, they can be shown in the stats web interface.
- Test files and simulators are included for checking how the system behaves with sample data.
