//https://github.com/clintdbrua/mqtttosql.git

// Import modules
const mqtt = require('mqtt');
const sql = require('mssql');

// Debugging
console.log("Starting MQTT to SQL script...");

// MQTT Broker Connection
const client = mqtt.connect('mqtt://localhost', { // Replace with your actual broker IP
  username: 'clintb',
  password: 'Hellohello',
});

client.on('connect', () => {
    console.log("Connected to MQTT broker.");
    client.subscribe('lincoln_electric/welding/100002021218015/summary', (err) => {
        if (err) {
            console.error("Subscription error:", err);
        } else {
            console.log("Subscribed to topic: lincoln_electric/welding/100002021218015/summary");
        }
    });
});

// Database configuration
const dbConfig = {
    user: 'clintb',
    password: 'F@ll1Shere!',
    server: 'appserver', // Change this
    database: 'MfgProduction',
    options: {
        encrypt: false, // Set to true if using Azure SQL
        trustServerCertificate: true
    }
};

// Create a pool of connections
let pool;

// Function to connect to the SQL database using pooling
async function connectToDatabase() {
    try {
        pool = await sql.connect(dbConfig);
        console.log("Connected to SQL database.");
    } catch (err) {
        console.error("Database connection error:", err);
    }
}

// Connect to the database initially
connectToDatabase();

// Reconnect logic for pool (to ensure the pool is always available)
async function ensureDatabaseConnection() {
    if (!pool || pool.closed) {
        console.log('Reconnecting to SQL database...');
        await connectToDatabase();
    }
}

client.on('message', async (topic, message) => {
    try {
        // Convert message to JSON
        const data = JSON.parse(message.toString());
        console.log(`Received message on topic: ${topic}`, data);

        // Ensure the database connection is active before proceeding
        await ensureDatabaseConnection();

        // Insert Data into SQL Table
        const query = `
            INSERT INTO WeldSummary (
                timestamp, weld_record_index, 
                current_average, current_min, current_max, current_pct_high, current_pct_low, 
                current_limit_high, current_limit_low, 
                voltage_average, voltage_min, voltage_max, voltage_pct_high, voltage_pct_low, 
                voltage_limit_high, voltage_limit_low, 
                wire_feed_speed_average, wire_feed_speed_min, wire_feed_speed_max, 
                wire_feed_speed_pct_high, wire_feed_speed_pct_low, 
                wire_feed_speed_limit_high, wire_feed_speed_limit_low, 
                using_weld_score, start_delay, end_delay, 
                duration_value, duration_limit_high, duration_limit_low, 
                consumable_density, consumable_diameter, 
                true_energy, weld_profile, weld_start_time, 
                status_current_low, status_current_high, 
                status_voltage_low, status_voltage_high, 
                status_wire_feed_speed_low, status_wire_feed_speed_high, 
                status_weld_score_low, status_arc_time_out_of_limits, 
                status_short_weld, status_arc_time_low, status_arc_time_high, 
                status_alarm, status_latch_alarm, status_fault, status_latch_fault, 
                limits_enabled_arc_time, limits_enabled_weld_score, 
                limits_enabled_wire_feed_speed, limits_enabled_current, limits_enabled_voltage, 
                part_serial, operator_id, consumable_lot, 
                weld_mode, assembly_id, seam_id, 
                average_motor_current, average_gas_flow, 
                warnings_wire_feed_speed_high, warnings_wire_feed_speed_low, 
                warnings_voltage_high, warnings_voltage_low, 
                warnings_current_high, warnings_current_low, 
                warnings_weld_score, warnings_time, 
                weld_type, wire_drive_sn
            ) VALUES (
                @timestamp, @weld_record_index, 
                @current_average, @current_min, @current_max, @current_pct_high, @current_pct_low, 
                @current_limit_high, @current_limit_low, 
                @voltage_average, @voltage_min, @voltage_max, @voltage_pct_high, @voltage_pct_low, 
                @voltage_limit_high, @voltage_limit_low, 
                @wire_feed_speed_average, @wire_feed_speed_min, @wire_feed_speed_max, 
                @wire_feed_speed_pct_high, @wire_feed_speed_pct_low, 
                @wire_feed_speed_limit_high, @wire_feed_speed_limit_low, 
                @using_weld_score, @start_delay, @end_delay, 
                @duration_value, @duration_limit_high, @duration_limit_low, 
                @consumable_density, @consumable_diameter, 
                @true_energy, @weld_profile, @weld_start_time, 
                @status_current_low, @status_current_high, 
                @status_voltage_low, @status_voltage_high, 
                @status_wire_feed_speed_low, @status_wire_feed_speed_high, 
                @status_weld_score_low, @status_arc_time_out_of_limits, 
                @status_short_weld, @status_arc_time_low, @status_arc_time_high, 
                @status_alarm, @status_latch_alarm, @status_fault, @status_latch_fault, 
                @limits_enabled_arc_time, @limits_enabled_weld_score, 
                @limits_enabled_wire_feed_speed, @limits_enabled_current, @limits_enabled_voltage, 
                @part_serial, @operator_id, @consumable_lot, 
                @weld_mode, @assembly_id, @seam_id, 
                @average_motor_current, @average_gas_flow, 
                @warnings_wire_feed_speed_high, @warnings_wire_feed_speed_low, 
                @warnings_voltage_high, @warnings_voltage_low, 
                @warnings_current_high, @warnings_current_low, 
                @warnings_weld_score, @warnings_time, 
                @weld_type, @wire_drive_sn
            )
        `;

        // Execute the query
        await pool.request()
            .input('timestamp', sql.BigInt, data.timestamp)
            .input('weld_record_index', sql.Int, data.weld_record_index)
            .input('current_average', sql.Float, data.current.average)
            .input('current_min', sql.Float, data.current.min)
            .input('current_max', sql.Float, data.current.max)
            .input('current_pct_high', sql.Int, data.current.pct_high)
            .input('current_pct_low', sql.Int, data.current.pct_low)
            .input('current_limit_high', sql.Float, data.current.limits.high)
            .input('current_limit_low', sql.Float, data.current.limits.low)
            .input('voltage_average', sql.Float, data.voltage.average)
            .input('voltage_min', sql.Float, data.voltage.min)
            .input('voltage_max', sql.Float, data.voltage.max)
            .input('voltage_pct_high', sql.Int, data.voltage.pct_high)
            .input('voltage_pct_low', sql.Int, data.voltage.pct_low)
            .input('voltage_limit_high', sql.Float, data.voltage.limits.high)
            .input('voltage_limit_low', sql.Float, data.voltage.limits.low)
            .input('wire_feed_speed_average', sql.Int, data.wire_feed_speed.average)
            .input('wire_feed_speed_min', sql.Int, data.wire_feed_speed.min)
            .input('wire_feed_speed_max', sql.Int, data.wire_feed_speed.max)
            .input('wire_feed_speed_pct_high', sql.Int, data.wire_feed_speed.pct_high)
            .input('wire_feed_speed_pct_low', sql.Int, data.wire_feed_speed.pct_low)
            .input('wire_feed_speed_limit_high', sql.Int, data.wire_feed_speed.limits.high)
            .input('wire_feed_speed_limit_low', sql.Int, data.wire_feed_speed.limits.low)
            .input('using_weld_score', sql.Bit, data.using_weld_score)
            .input('weld_type', sql.NVarChar, data.weld_type)
            .input('wire_drive_sn', sql.NVarChar, data.wire_drive_sn)
            .query(query);

        console.log("Data successfully inserted into database.");

    } catch (error) {
        console.error("Error processing message:", error);
    }
});
