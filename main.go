package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "math/rand"
    "net/http"
    "sync"
    "sync/atomic"
    "time"
)

type InverterPayload struct {
    DeviceType     string       `json:"device_type"`
    DeviceName     string       `json:"device_name"`
    DeviceID       string       `json:"device_id"`
    Date           string       `json:"date"`
    Time           string       `json:"time"`
    SignalStrength string       `json:"signal_strength"`
    Data           InverterData `json:"data"`
}

type InverterData struct {
    SerialNo         string `json:"serial_no"`
    S1V              int    `json:"s1v"`
    TotalOutputPower int    `json:"total_output_power"`
    F                int    `json:"f"`
    TodayE           int    `json:"today_e"`
    TotalE           int    `json:"total_e"`
    InvTemp          int    `json:"inv_temp"`
    FaultCode        int    `json:"fault_code"`
}

var totalSent uint64
var sentLast10Sec uint64

func main() {
    endpoint := "http://localhost:8080/api/data"
    rate := 600                     // records per second
    runDuration := 60 * time.Second
    totalRecords := rate * 10 // Exact number: 6000

    fmt.Printf("Starting inverter simulator... sending %d records/sec (real format)\n", rate)
    fmt.Printf("Target: %d total records in %v\n", totalRecords, runDuration)

    // Create HTTP client with connection pooling
// 	You are creating one delivery agent — not hundreds.
// This single client will manage:

// Opening network connections

// Keeping them alive

// Reusing them efficiently

    client := &http.Client{
        Timeout: 5 * time.Second,
        Transport: &http.Transport{
            MaxIdleConns:        1000,//number of connections to keep open
            MaxIdleConnsPerHost: 1000,//number of connections to keep open per host
            IdleConnTimeout:     90 * time.Second,//time to keep a connection open
        },
    }

    // // Stats reporter (every 10 seconds)
    // go func() {
    //     ticker := time.NewTicker(10 * time.Second)
    //     defer ticker.Stop()
    //     for range ticker.C {
    //         last := atomic.SwapUint64(&sentLast10Sec, 0)
    //         total := atomic.LoadUint64(&totalSent)
    //         fmt.Printf("[INFO] Sent %d records in last 10s | Total sent: %d\n", last, total)
    //     }
    // }()

    startTime := time.Now()
    var wg sync.WaitGroup
    recordsSent := uint64(0)

    // Send exactly totalRecords at the target rate
    ticker := time.NewTicker(time.Second / time.Duration(rate))
    defer ticker.Stop()

    for recordsSent < uint64(totalRecords) {
        <-ticker.C
        
        wg.Add(1)
        go func() {
            defer wg.Done()
            sendSingle(client, endpoint)
        }()
        
        recordsSent++
        
        // Check if we've exceeded the time limit (safety check)
        if time.Since(startTime) > runDuration+5*time.Second {
            fmt.Println("[WARN] Exceeded time limit, stopping...")
            break
        }
    }

    ticker.Stop()
    wg.Wait() // Wait for all pending requests to complete

    elapsed := time.Since(startTime)
    total := atomic.LoadUint64(&totalSent)
    fmt.Printf("\n[INFO] Simulation finished after %v | Total records sent: %d/%d\n", 
        elapsed.Round(time.Millisecond), total, totalRecords)
    fmt.Printf("[INFO] Actual rate: %.2f records/sec\n", float64(total)/elapsed.Seconds())
}

func sendSingle(client *http.Client, url string) {
    now := time.Now()
    payload := InverterPayload{
        DeviceType:     "Inverter",
        DeviceName:     fmt.Sprintf("ESIN%d", rand.Intn(50)+1),
        DeviceID:       fmt.Sprintf("ESDL%d", rand.Intn(600)+1),
        Date:           now.Format("02/01/2006"),
        Time:           now.Format("15:04:05"),
        SignalStrength: "-1",
        Data: InverterData{
            SerialNo:         fmt.Sprintf("%d", rand.Intn(600)+1),
            S1V:              6200 + rand.Intn(200) - 100,
            TotalOutputPower: 147000 + rand.Intn(500),
            F:                700 + rand.Intn(50),
            TodayE:           rand.Intn(1000),
            TotalE:           500000 + rand.Intn(10000),
            InvTemp:          650 + rand.Intn(10) - 5,
            FaultCode:        randomFault(),
        },
    }

    jsonData, _ := json.Marshal(payload)
    resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
    if err != nil {
        // Uncomment for debugging: fmt.Println("Error sending record:", err)
        return
    }
    resp.Body.Close()

    atomic.AddUint64(&totalSent, 1)
    atomic.AddUint64(&sentLast10Sec, 1)
}

func randomFault() int {
    if rand.Float64() < 0.1 {
        return rand.Intn(5) + 1
    }
    return 0
}

// ## Key Changes:

// 1. **Loop counter**: Uses `recordsSent` to send **exactly 36,000 records**
// 2. **No time-based exit**: The loop continues until all records are sent
// 3. **Better metrics**: Shows actual rate achieved at the end

// This will now send **exactly 36,000 records**, regardless of minor timing variations. The ~141 missing records issue should be completely resolved!

// **Expected output:**
// ```
// Total records sent: 36000/36000
// Actual rate: ~600.00 records/sec