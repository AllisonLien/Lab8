USE AdventureWorks2019;
GO

--Task 1: Create IoT Schema and Operational Tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'IoT')
    EXEC('CREATE SCHEMA IoT');
GO

IF OBJECT_ID('IoT.Device','U') IS NOT NULL
    DROP TABLE IoT.Device;

IF OBJECT_ID('IoT.Telemetry','U') IS NOT NULL
    DROP TABLE IoT.Telemetry;

IF OBJECT_ID('IoT.DeviceHealth','U') IS NOT NULL
    DROP TABLE IoT.DeviceHealth;

IF OBJECT_ID('IoT.Alert','U') IS NOT NULL
    DROP TABLE IoT.Alert;

IF OBJECT_ID('IoT.TelemetryErrorLog','U') IS NOT NULL
    DROP TABLE IoT.TelemetryErrorLog;

IF OBJECT_ID('IoT.TelemetryAudit','U') IS NOT NULL
    DROP TABLE IoT.TelemetryAudit;
go

CREATE TABLE IoT.Device
(
    DeviceID         INT IDENTITY(1,1) CONSTRAINT PK_IoT_Device PRIMARY KEY,
    SerialNumber     NVARCHAR(50)  NOT NULL CONSTRAINT UQ_IoT_Device_Serial UNIQUE,
    DeviceType       NVARCHAR(50)  NOT NULL,
    FirmwareVersion  NVARCHAR(20)  NOT NULL,
    RegistrationDate DATETIME2(0)  NOT NULL CONSTRAINT DF_IoT_Device_RegistrationDate DEFAULT (SYSUTCDATETIME()),
    LastHeartbeat    DATETIME2(0)  NULL,
    Status           NVARCHAR(20)  NOT NULL 
    CONSTRAINT DF_IoT_Device_Status DEFAULT('Active'),
    CONSTRAINT CK_IoT_Device_Status CHECK (Status IN ('Active','Inactive','Maintenance','Decommissioned'))
);
GO

CREATE TABLE IoT.Telemetry
(
    TelemetryID   INT IDENTITY(1,1) CONSTRAINT PK_IoT_Telemetry PRIMARY KEY,
    DeviceID      INT           NOT NULL,
    [Timestamp]   DATETIME2(3)  NOT NULL,
    Speed         DECIMAL(6,2)  NULL,
    Cadence       INT           NULL,
    Temperature   DECIMAL(5,2)  NULL,
    BatteryLevel  TINYINT       NULL,
    GPSLatitude   DECIMAL(9,6)  NULL,
    GPSLongitude  DECIMAL(9,6)  NULL,
    CONSTRAINT FK_IoT_Telemetry_Device 
    FOREIGN KEY (DeviceID) REFERENCES IoT.Device(DeviceID)
);
GO

CREATE TABLE IoT.DeviceHealth
(
    HealthID          INT IDENTITY(1,1) CONSTRAINT PK_IoT_DeviceHealth PRIMARY KEY,
    DeviceID          INT           NOT NULL,
    [Timestamp]       DATETIME2(3)  NOT NULL,
    IsHealthy         BIT           NOT NULL,
    TemperatureStatus NVARCHAR(20)  NOT NULL,
    BatteryStatus     NVARCHAR(20)  NOT NULL,
    VibrationStatus   NVARCHAR(20)  NOT NULL,
    CONSTRAINT FK_IoT_DeviceHealth_Device
        FOREIGN KEY (DeviceID) REFERENCES IoT.Device(DeviceID)
);
GO

CREATE TABLE IoT.Alert
(
    AlertID     INT IDENTITY(1,1) CONSTRAINT PK_IoT_Alert PRIMARY KEY,
    DeviceID    INT           NOT NULL,
    [Timestamp] DATETIME2(3)  NOT NULL,
    AlertType   NVARCHAR(50)  NOT NULL,
    Severity    NVARCHAR(20)  NOT NULL,
    [Description] NVARCHAR(400) NULL,
    CONSTRAINT FK_IoT_Alert_Device
        FOREIGN KEY (DeviceID) REFERENCES IoT.Device(DeviceID)
);
GO

CREATE TABLE IoT.TelemetryErrorLog
(
    ErrorID      INT IDENTITY(1,1) CONSTRAINT PK_IoT_TelemetryErrorLog PRIMARY KEY,
    DeviceID     INT           NULL,
    [Timestamp]  DATETIME2(3)  NOT NULL CONSTRAINT DF_IoT_Error_Timestamp DEFAULT (SYSUTCDATETIME()),
    ErrorMessage NVARCHAR(4000) NOT NULL,
    ErrorStep    NVARCHAR(100)  NULL,
    RawPayload   NVARCHAR(MAX)  NULL,
    FOREIGN KEY (DeviceID) REFERENCES IoT.Device(DeviceID)
);
GO




CREATE TABLE IoT.TelemetryAudit
(
    AuditID           INT IDENTITY(1,1) CONSTRAINT PK_IoT_TelemetryAudit PRIMARY KEY,
    DeviceID          INT           NOT NULL,
    [Timestamp]       DATETIME2(3)  NOT NULL,
    ProcessingDuration INT          NOT NULL,   
    RecordsInserted   INT           NOT NULL,
    [Status]          NVARCHAR(50)  NOT NULL,
    CONSTRAINT FK_IoT_TelemetryAudit_Device
    FOREIGN KEY (DeviceID) REFERENCES IoT.Device(DeviceID)
);
GO

--Task 2: Implement Multi-Step IoT Telemetry Transaction
IF OBJECT_ID('IoT.usp_ProcessTelemetryCycle', 'P') IS NOT NULL
    DROP PROCEDURE IoT.usp_ProcessTelemetryCycle;
GO

CREATE PROCEDURE IoT.usp_ProcessTelemetryCycle
    @DeviceID     INT,
    @Speed        DECIMAL(6,2),
    @Cadence      INT,
    @Temperature  DECIMAL(5,2),
    @BatteryLevel TINYINT,
    @Latitude     DECIMAL(9,6),
    @Longitude    DECIMAL(9,6),
    @RawPayload   NVARCHAR(MAX) = NULL  
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @StartTime       DATETIME2(3) = SYSUTCDATETIME(),
        @Now             DATETIME2(3),
        @RecordsInserted INT = 0,
        @ErrorStep       NVARCHAR(100) = N'',
        @TemperatureStatus NVARCHAR(20),
        @BatteryStatus     NVARCHAR(20),
        @VibrationStatus   NVARCHAR(20) = N'Normal',
        @HasAlert        BIT = 0;

    BEGIN TRY
        SET @ErrorStep = N'Validate Device';　--check deviceID

        IF NOT EXISTS (
            SELECT 1 
            FROM IoT.Device 
            WHERE DeviceID = @DeviceID
              AND Status <> 'Decommissioned'
        )
        BEGIN
            THROW 51001, 'Device not registered or is decommissioned.', 1;
        END;

        SET @ErrorStep = N'Validate input ranges';

        IF @Temperature < -40 OR @Temperature > 85
            THROW 51002, 'Temperature reading out of sensor range (-40 to 85).', 1;

        IF @BatteryLevel < 0 OR @BatteryLevel > 100
            THROW 51003, 'BatteryLevel must be between 0 and 100.', 1;

        IF @Latitude IS NULL OR @Longitude IS NULL
            THROW 51004, 'GPS coordinates cannot be NULL.', 1;

        IF @Latitude  < -90 OR @Latitude  > 90
            THROW 51005, 'Latitude must be between -90 and 90.', 1;

        IF @Longitude < -180 OR @Longitude > 180
            THROW 51006, 'Longitude must be between -180 and 180.', 1;

       
        BEGIN TRANSACTION;

        SET @Now = SYSUTCDATETIME();
        SET @ErrorStep = N'Insert IoT.Telemetry';

        INSERT INTO IoT.Telemetry
        (
            DeviceID, [Timestamp], Speed, Cadence,
            Temperature, BatteryLevel, GPSLatitude, GPSLongitude
        )
        VALUES
        (
            @DeviceID, @Now, @Speed, @Cadence,
            @Temperature, @BatteryLevel, @Latitude, @Longitude
        );

        SET @RecordsInserted += 1;


        SET @ErrorStep = N'Update IoT.Device.LastHeartbeat';

        UPDATE IoT.Device
        SET LastHeartbeat = @Now
        WHERE DeviceID = @DeviceID;

       
        SET @ErrorStep = N'Insert IoT.DeviceHealth';

        SET @TemperatureStatus = CASE 
            WHEN @Temperature BETWEEN -10 AND 60 THEN N'Normal'
            WHEN @Temperature > 60 THEN N'High'
            ELSE N'Low'
        END;

        SET @BatteryStatus = CASE 
            WHEN @BatteryLevel >= 50 THEN N'Good'
            WHEN @BatteryLevel BETWEEN 20 AND 49 THEN N'Low'
            ELSE N'Critical'
        END;

        INSERT INTO IoT.DeviceHealth
        (
            DeviceID, [Timestamp], IsHealthy,
            TemperatureStatus, BatteryStatus, VibrationStatus
        )
        VALUES
        (
            @DeviceID, @Now,
            CASE WHEN @TemperatureStatus = N'Normal' AND @BatteryStatus <> N'Critical' THEN 1 ELSE 0 END,
            @TemperatureStatus, @BatteryStatus, @VibrationStatus
        );

        SET @RecordsInserted += 1;
        SET @ErrorStep = N'Insert IoT.Alert';
        IF @BatteryStatus IN (N'Low', N'Critical')
        BEGIN
            INSERT INTO IoT.Alert
            (
                DeviceID, [Timestamp], AlertType, Severity, [Description]
            )
            VALUES
            (
                @DeviceID, @Now,
                N'Battery',
                CASE WHEN @BatteryStatus = N'Critical' THEN N'High' ELSE N'Medium' END,
                CONCAT(N'Battery status: ', @BatteryStatus, N' (', @BatteryLevel, N'%)')
            );

            SET @RecordsInserted += 1;
            SET @HasAlert = 1;
        END;

        IF @TemperatureStatus <> N'Normal'
        BEGIN
            INSERT INTO IoT.Alert
            (
                DeviceID, [Timestamp], AlertType, Severity, [Description]
            )
            VALUES
            (
                @DeviceID, @Now,
                N'Temperature',
                N'High',
                CONCAT(N'Temperature status: ', @TemperatureStatus, N' (', @Temperature, N'°C)')
            );

            SET @RecordsInserted += 1;
            SET @HasAlert = 1;
        END;

        SET @ErrorStep = N'Insert IoT.TelemetryAudit';

        DECLARE @DurationMs INT = DATEDIFF(MILLISECOND, @StartTime, SYSUTCDATETIME());

        INSERT INTO IoT.TelemetryAudit
        (
            DeviceID, [Timestamp], ProcessingDuration, RecordsInserted, [Status]
        )
        VALUES
        (
            @DeviceID, @Now, @DurationMs, @RecordsInserted,
            CASE WHEN @HasAlert = 1 THEN N'CompletedWithAlerts' ELSE N'Completed' END
        );

     
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH

        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();

        INSERT INTO IoT.TelemetryErrorLog
        (
            DeviceID, [Timestamp], ErrorMessage, ErrorStep, RawPayload
        )
        VALUES
        (
            @DeviceID,
            SYSUTCDATETIME(),
            @ErrorMessage,
            @ErrorStep,
            @RawPayload
        );
        THROW;
    END CATCH
END;
GO

--Task 3: Failure Scenario Demonstration
--invalid DeviceID
EXEC IoT.usp_ProcessTelemetryCycle
    @DeviceID     = 99999,
    @Speed        = 10,
    @Cadence      = 80,
    @Temperature  = 30,
    @BatteryLevel = 50,
    @Latitude     = 43.6532,
    @Longitude    = -79.3832,
    @RawPayload   = N'Case1_InvalidDeviceID';

SELECT TOP 5 * FROM IoT.Telemetry ORDER BY TelemetryID DESC;
SELECT TOP 5 * FROM IoT.DeviceHealth ORDER BY HealthID DESC;
SELECT TOP 5 * FROM IoT.Alert ORDER BY AlertID DESC;
SELECT TOP 5 * FROM IoT.TelemetryAudit ORDER BY AuditID DESC;

SELECT TOP 5 * FROM IoT.TelemetryErrorLog ORDER BY ErrorID DESC;

--bettery level>100
INSERT INTO IoT.Device
(SerialNumber, DeviceType, FirmwareVersion)
VALUES ('SN-001', 'SmartBike', '1.0.0');

EXEC IoT.usp_ProcessTelemetryCycle
    @DeviceID     = 1,
    @Speed        = 20,
    @Cadence      = 60,
    @Temperature  = 25,
    @BatteryLevel = 150,  
    @Latitude     = 43.6532,
    @Longitude    = -79.3832,
    @RawPayload   = N'Case2_BatteryAbove100';

SELECT TOP 5 * FROM IoT.Telemetry ORDER BY TelemetryID DESC;
SELECT TOP 5 * FROM IoT.DeviceHealth ORDER BY HealthID DESC;
SELECT TOP 5 * FROM IoT.Alert ORDER BY AlertID DESC;
SELECT TOP 5 * FROM IoT.TelemetryAudit ORDER BY AuditID DESC;

SELECT TOP 5 * FROM IoT.TelemetryErrorLog ORDER BY ErrorID DESC;

--Latitude = NULL
EXEC IoT.usp_ProcessTelemetryCycle
    @DeviceID     = 1,
    @Speed        = 20,
    @Cadence      = 90,
    @Temperature  = 22,
    @BatteryLevel = 80,
    @Latitude     = NULL,          
    @Longitude    = -79.3832,
    @RawPayload   = N'Case3_LatitudeNULL';

SELECT TOP 5 * FROM IoT.Telemetry ORDER BY TelemetryID DESC;
SELECT TOP 5 * FROM IoT.DeviceHealth ORDER BY HealthID DESC;
SELECT TOP 5 * FROM IoT.Alert ORDER BY AlertID DESC;
SELECT TOP 5 * FROM IoT.TelemetryAudit ORDER BY AuditID DESC;

SELECT TOP 5 * FROM IoT.TelemetryErrorLog ORDER BY ErrorID DESC;

--Task 4: Concurrency Requirements for IoT Device State
BEGIN TRAN;

UPDATE IoT.Device
SET Status = 'Maintenance'
WHERE DeviceID = 1;

COMMIT;

--Lock Device
BEGIN TRAN;

UPDATE IoT.Device
SET LastHeartbeat = SYSDATETIME()
WHERE DeviceID = 1;
COMMIT;

--Isolation Level
BEGIN TRAN;

INSERT INTO IoT.Telemetry
(DeviceID, [Timestamp], Speed, Cadence, Temperature, BatteryLevel, GPSLatitude, GPSLongitude)
VALUES (1, SYSDATETIME(), 100, 90, 30, 80, 43.6532, -79.3832);
COMMIT;

--Task5:Department-Based IoT Role Design
CREATE ROLE IoTDeviceAgent;
CREATE ROLE TelemetryIngestionService;
CREATE ROLE IoTAnalyst;
CREATE ROLE IoTFieldTechnician;
CREATE ROLE SecurityComplianceOfficer;
GO
USE master;
GO
CREATE LOGIN DeviceAgentLogin WITH PASSWORD = 'Password1!';
CREATE LOGIN IngestionLogin WITH PASSWORD = 'Password1!';
CREATE LOGIN AnalystLogin WITH PASSWORD = 'Password1!';
CREATE LOGIN TechLogin WITH PASSWORD = 'Password1!';
CREATE LOGIN ComplianceLogin WITH PASSWORD = 'Password1!';
GO

USE AdventureWorks2019;
GO
CREATE USER DeviceAgentUser     FOR LOGIN DeviceAgentLogin;
CREATE USER IngestionUser       FOR LOGIN IngestionLogin;
CREATE USER AnalystUser         FOR LOGIN AnalystLogin;
CREATE USER TechUser            FOR LOGIN TechLogin;
CREATE USER ComplianceUser      FOR LOGIN ComplianceLogin;

EXEC sp_addrolemember 'IoTDeviceAgent',            'DeviceAgentUser';
EXEC sp_addrolemember 'TelemetryIngestionService', 'IngestionUser';
EXEC sp_addrolemember 'IoTAnalyst',                'AnalystUser';
EXEC sp_addrolemember 'IoTFieldTechnician',        'TechUser';
EXEC sp_addrolemember 'SecurityComplianceOfficer', 'ComplianceUser';
GO
--ROLE 1：IoTDeviceAgent
-- Allow manage devices
GRANT INSERT, UPDATE ON IoT.Device TO IoTDeviceAgent;
GRANT SELECT ON IoT.Device TO IoTDeviceAgent;
-- Deny write on telemetry-related tables
DENY INSERT, UPDATE, DELETE ON IoT.Telemetry TO IoTDeviceAgent;
DENY INSERT, UPDATE, DELETE ON IoT.DeviceHealth TO IoTDeviceAgent;
DENY INSERT, UPDATE, DELETE ON IoT.Alert TO IoTDeviceAgent;
DENY INSERT, UPDATE, DELETE ON IoT.TelemetryAudit TO IoTDeviceAgent;
DENY INSERT, UPDATE, DELETE ON IoT.TelemetryErrorLog TO IoTDeviceAgent;
--ROLE 2：TelemetryIngestionService
-- insert
GRANT INSERT ON IoT.Telemetry TO TelemetryIngestionService;
GRANT INSERT ON IoT.DeviceHealth TO TelemetryIngestionService;
GRANT INSERT ON IoT.Alert TO TelemetryIngestionService;
GRANT INSERT ON IoT.TelemetryAudit TO TelemetryIngestionService;
GRANT SELECT ON IoT.Device TO TelemetryIngestionService;

-- allow only heartbeat update
GRANT UPDATE ON IoT.Device(LastHeartbeat) TO TelemetryIngestionService;
-- deny modifying
DENY UPDATE ON IoT.Device(DeviceType, FirmwareVersion, Status, SerialNumber) TO TelemetryIngestionService;
--ROLE 3：IoTAnalyst
-- read-only permissions
GRANT SELECT ON IoT.Device TO IoTAnalyst;
GRANT SELECT ON IoT.Telemetry TO IoTAnalyst;
GRANT SELECT ON IoT.DeviceHealth TO IoTAnalyst;
GRANT SELECT ON IoT.Alert TO IoTAnalyst;
GRANT SELECT ON IoT.TelemetryAudit TO IoTAnalyst;
GRANT SELECT ON IoT.TelemetryErrorLog TO IoTAnalyst;
-- deny any modification
DENY INSERT, UPDATE, DELETE ON IoT.Device TO IoTAnalyst;
DENY INSERT, UPDATE, DELETE ON IoT.Telemetry TO IoTAnalyst;
DENY INSERT, UPDATE, DELETE ON IoT.DeviceHealth TO IoTAnalyst;
DENY INSERT, UPDATE, DELETE ON IoT.Alert TO IoTAnalyst;
DENY INSERT, UPDATE, DELETE ON IoT.TelemetryAudit TO IoTAnalyst;
DENY INSERT, UPDATE, DELETE ON IoT.TelemetryErrorLog TO IoTAnalyst;
--ROLE 4：IoTFieldTechnician
DENY SELECT ON IoT.Device TO IoTFieldTechnician;
DENY SELECT ON IoT.Telemetry TO IoTFieldTechnician;
DENY SELECT ON IoT.DeviceHealth TO IoTFieldTechnician;
DENY SELECT ON IoT.Alert TO IoTFieldTechnician;
DENY SELECT ON IoT.TelemetryAudit TO IoTFieldTechnician;
DENY SELECT ON IoT.TelemetryErrorLog TO IoTFieldTechnician;
--ROLE 5：SecurityComplianceOfficer
GRANT SELECT ON IoT.Device TO SecurityComplianceOfficer;
GRANT SELECT ON IoT.Telemetry TO SecurityComplianceOfficer;
GRANT SELECT ON IoT.DeviceHealth TO SecurityComplianceOfficer;
GRANT SELECT ON IoT.Alert TO SecurityComplianceOfficer;
GRANT SELECT ON IoT.TelemetryAudit TO SecurityComplianceOfficer;
GRANT SELECT ON IoT.TelemetryErrorLog TO SecurityComplianceOfficer;
DENY INSERT, UPDATE, DELETE ON IoT.Device TO SecurityComplianceOfficer;
DENY INSERT, UPDATE, DELETE ON IoT.Telemetry TO SecurityComplianceOfficer;
DENY INSERT, UPDATE, DELETE ON IoT.DeviceHealth TO SecurityComplianceOfficer;
DENY INSERT, UPDATE, DELETE ON IoT.Alert TO SecurityComplianceOfficer;
DENY INSERT, UPDATE, DELETE ON IoT.TelemetryAudit TO SecurityComplianceOfficer;
DENY INSERT, UPDATE, DELETE ON IoT.TelemetryErrorLog TO SecurityComplianceOfficer;


--REVERT;

--Task 6：Permission Validation
--test role：IoTDeviceAgent
EXECUTE AS USER = 'DeviceAgentUser';

-- INSERT Device
INSERT INTO IoT.Device (SerialNumber, DeviceType, FirmwareVersion)
VALUES ('TEST-DA-001', 'SmartBike', '2.0');

-- UPDATE Device
UPDATE IoT.Device
SET FirmwareVersion = '2.1'
WHERE SerialNumber = 'TEST-DA-001';

--INSERT Telemetry
INSERT INTO IoT.Telemetry (DeviceID, [Timestamp], Speed)
VALUES (1, SYSUTCDATETIME(), 10);

--UPDATE Telemetry
UPDATE IoT.Telemetry
SET Speed = 99
WHERE TelemetryID = 1;

REVERT;
--Role 2：TelemetryIngestionService
EXECUTE AS USER = 'IngestionUser';

INSERT INTO IoT.Telemetry (DeviceID, [Timestamp], Speed)
VALUES (1, SYSUTCDATETIME(), 15);

UPDATE IoT.Device
SET LastHeartbeat = SYSUTCDATETIME()
WHERE DeviceID = 1;

UPDATE IoT.Device
SET FirmwareVersion = '999.9'
WHERE DeviceID = 1;
REVERT;
--Role 3：IoTAnalyst（Read-Only）
EXECUTE AS USER = 'AnalystUser';
SELECT TOP 5 * FROM IoT.Telemetry;

INSERT INTO IoT.Telemetry (DeviceID, [Timestamp], Speed)
VALUES (1, SYSUTCDATETIME(), 22);

UPDATE IoT.DeviceHealth
SET BatteryStatus = 'Low'
WHERE DeviceID = 1;
revert;
--Role 4：IoTFieldTechnician
EXECUTE AS USER = 'TechUser';
SELECT TOP 5 * FROM IoT.Device;

SELECT TOP 5 * FROM IoT.Telemetry;
REVERT;
--Role 5：SecurityComplianceOfficer
EXECUTE AS USER = 'ComplianceUser';
SELECT TOP 5 * FROM IoT.TelemetryAudit;

INSERT INTO IoT.TelemetryErrorLog (ErrorMessage)
VALUES ('fake error');

UPDATE IoT.Telemetry
SET Speed = 999
WHERE TelemetryID = 1;
revert;
