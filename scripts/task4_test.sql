use AdventureWorks2019;
go

UPDATE IoT.Device
SET Status = 'Active'
WHERE DeviceID = 1;

--Lock Device
EXEC IoT.usp_ProcessTelemetryCycle
    @DeviceID     = 1,
    @Speed        = 10,
    @Cadence      = 80,
    @Temperature  = 30,
    @BatteryLevel = 50,
    @Latitude     = 43.6532,
    @Longitude    = -79.3832,
    @RawPayload   = N'BlockingTest';

--Isolation Level
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT * FROM IoT.Telemetry WHERE DeviceID = 1;

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT * FROM IoT.Telemetry WHERE DeviceID = 1;

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT * FROM IoT.Telemetry WHERE DeviceID = 1;

USE master;
GO
ALTER DATABASE AdventureWorks2019 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
ALTER DATABASE AdventureWorks2019 SET ALLOW_SNAPSHOT_ISOLATION ON;
go
ALTER DATABASE AdventureWorks2019 SET MULTI_USER;
GO
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
SELECT * FROM IoT.Telemetry WHERE DeviceID = 1;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM IoT.Telemetry WHERE DeviceID = 1;
