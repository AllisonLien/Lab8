--Task 4: Concurrency Requirements for IoT Device State
BEGIN TRAN;

UPDATE IoT.Device
SET Status = 'Maintenance'
WHERE DeviceID = 1;



--Lock Device
BEGIN TRAN;

UPDATE IoT.Device
SET LastHeartbeat = SYSDATETIME()
WHERE DeviceID = 1;


--Isolation Level
BEGIN TRAN;

INSERT INTO IoT.Telemetry
(DeviceID, [Timestamp], Speed, Cadence, Temperature, BatteryLevel, GPSLatitude, GPSLongitude)
VALUES (1, SYSDATETIME(), 100, 90, 30, 80, 43.6532, -79.3832);
