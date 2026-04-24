import os
import json
import hashlib
import requests
import boto3
from botocore.exceptions import ClientError

# Global client instances
s3_client = None

# NIST 800-61R3 Incident Response Phases
PHASE_PREPARATION = "PREPARATION"
PHASE_DETECTION = "DETECTION"
PHASE_CONTAINMENT = "CONTAINMENT"
PHASE_ERADICATION = "ERADICATION"
PHASE_RECOVERY = "RECOVERY"
PHASE_POST_INCIDENT = "POST-INCIDENT"

# NIST Attack Vectors
VECTOR_MALICIOUS_CODE = "Malicious Code"

def init(ctx):
    """
    One-time initialization: Setup S3 client and load configuration.
    """
    global s3_client
    
    log_nist_phase(ctx, PHASE_PREPARATION, "Initializing ThreatDefense Function")
    
    # Load and log environment variables (masked)
    vt_api_key = os.environ.get('VT_API_KEY', '')
    bad_hashes = os.environ.get('BAD_HASHES', '')
    
    ctx.logger.info("📋 CONFIGURATION LOADED:")
    if vt_api_key:
        ctx.logger.info(f"  VT_API_KEY: {vt_api_key[:4]}...{vt_api_key[-4:]} (Length: {len(vt_api_key)})")
    else:
        ctx.logger.warning("  ⚠️ VT_API_KEY is missing! VirusTotal lookups will fail.")
        
    ctx.logger.info(f"  BAD_HASHES: {len(bad_hashes.split(',')) if bad_hashes else 0} signatures loaded")
    ctx.logger.info(f"  ENABLE_VT_UPLOAD: {os.environ.get('ENABLE_VT_UPLOAD', 'false')}")
    ctx.logger.info(f"  SERVICENOW_URL: {os.environ.get('SERVICENOW_URL', 'Not Set')}")

    # Initialize S3
    try:
        s3_endpoint = os.environ.get('S3_ENDPOINT', 'https://s3.amazonaws.com')
        s3_access_key = os.environ.get('S3_ACCESS_KEY', '')
        s3_secret_key = os.environ.get('S3_SECRET_KEY', '')
        
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key
        )
        log_nist_phase(ctx, PHASE_PREPARATION, f"S3 Client Initialized (Endpoint: {s3_endpoint})")
        
    except Exception as e:
        ctx.logger.error(f"❌ FAILED to initialize S3 client: {str(e)}")
        raise

def handler(ctx, event):
    """
    Main event handler for file scanning and remediation.
    """
    log_nist_phase(ctx, PHASE_DETECTION, "Event Received - Starting Analysis")
    
    try:
        # 1. Parse Event
        s3_bucket, s3_key = parse_event(ctx, event)
        if not s3_bucket or not s3_key:
            ctx.logger.error("Could not determine file location from event.")
            return {"status": "error", "message": "Missing file location"}

        ctx.logger.info(f"🔍 Analyzing object: s3://{s3_bucket}/{s3_key}")

        # 2. Calculate Hash (Detection Phase)
        file_hash, file_size = compute_s3_hash(ctx, s3_bucket, s3_key)
        if not file_hash:
            return {"status": "error", "message": "Failed to compute hash"}
            
        ctx.logger.info(f"#️⃣ SHA256: {file_hash} (Size: {file_size} bytes)")

        # 3. Threat Logic (Detection Phase)
        threat_status, threat_details = analyze_threat(ctx, file_hash, file_size)
        
        # 4. Remediation
        if threat_status == "MALICIOUS":
            handle_malicious_file(ctx, s3_bucket, s3_key, file_hash, threat_details)
            return {
                "status": "threat_detected", 
                "verdict": "MALICIOUS", 
                "action": "deleted",
                "details": threat_details
            }
        elif threat_status == "UNKNOWN":
            log_nist_phase(ctx, PHASE_DETECTION, f"File Verdict: UNKNOWN. Hash: {file_hash}")
            # Optional: Logic to upload if enabled would go here
            if os.environ.get('ENABLE_VT_UPLOAD', 'false').lower() == 'true':
                ctx.logger.info("Upload enabled - initiating upload to VirusTotal (NOT IMPLEMENTED IN THIS STEP)")
            return {"status": "clean", "verdict": "UNKNOWN", "hash": file_hash}
        else:
            log_nist_phase(ctx, PHASE_DETECTION, f"File Verdict: CLEAN. Hash: {file_hash}")
            return {"status": "clean", "verdict": "CLEAN", "hash": file_hash}

    except Exception as e:
        ctx.logger.error(f"❌ Unhandled exception: {str(e)}")
        ctx.logger.exception(e)
        return {"status": "error", "message": str(e)}

def parse_event(ctx, event):
    """Extracts bucket and key from Element or Generic events."""
    s3_bucket = None
    s3_key = None
    
    if hasattr(event, 'type') and event.type == "Element":
        try:
            element_event = event.as_element_event()
            s3_bucket = element_event.bucket
            s3_key = element_event.object_key
            ctx.logger.info("Parsed Element Event")
        except Exception as e:
            ctx.logger.warning(f"Failed to parse Element event: {e}")
    
    if not s3_bucket:
        # Fallback to data payload
        data = event.get_data() if hasattr(event, 'get_data') else {}
        s3_bucket = data.get('s3_bucket')
        s3_key = data.get('s3_key')
        if s3_bucket:
            ctx.logger.info("Parsed Generic Event payload")
            
    return s3_bucket, s3_key

def compute_s3_hash(ctx, bucket, key):
    """Streams file from S3 and computes SHA256 hash."""
    sha256_hash = hashlib.sha256()
    file_size = 0
    try:
        s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
        stream = s3_obj['Body']
        
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            sha256_hash.update(chunk)
            file_size += len(chunk)
            
        return sha256_hash.hexdigest(), file_size
    except ClientError as e:
        ctx.logger.error(f"S3 Access Error: {e}")
        return None, 0
    except Exception as e:
        ctx.logger.error(f"Hash computation error: {e}")
        return None, 0

def analyze_threat(ctx, file_hash, file_size):
    """
    Determines if a file is malicious via Bad Hash List or VirusTotal.
    Returns: (status, details_dict)
    Status: CLEAN, MALICIOUS, UNKNOWN
    """
    # 1. Check Internal Bad Hash List (Simulation)
    bad_hashes_env = os.environ.get('BAD_HASHES', '')
    bad_list = [h.strip().lower() for h in bad_hashes_env.split(',') if h.strip()]
    
    if file_hash.lower() in bad_list:
        log_nist_phase(ctx, PHASE_DETECTION, f"🚨 KNOWN BAD HASH DETECTED (Internal List): {file_hash}")
        return "MALICIOUS", {
            "source": "INTERNAL_BLACKLIST", 
            "threat_label": "SIMULATED_THREAT",
            "score": 100
        }

    # 2. Check VirusTotal
    vt_key = os.environ.get('VT_API_KEY')
    if not vt_key:
        ctx.logger.warning("Skipping VirusTotal check (No API Key)")
        return "UNKNOWN", {"source": "NONE", "reason": "NO_KEY"}
        
    try:
        url = f"https://www.virustotal.com/api/v3/files/{file_hash}"
        headers = {"x-apikey": vt_key}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            attributes = data.get('data', {}).get('attributes', {})
            stats = attributes.get('last_analysis_stats', {})
            malicious_count = stats.get('malicious', 0)
            suspicious_count = stats.get('suspicious', 0)
            
            verdict = "CLEAN"
            if malicious_count > 0:
                verdict = "MALICIOUS"
                log_nist_phase(ctx, PHASE_DETECTION, f"🚨 VIRUSTOTAL DETECTION: {malicious_count} engines flagged this file.")
            else:
                ctx.logger.info(f"VirusTotal clean. (Malicious: {malicious_count})")
                
            return verdict, {
                "source": "VIRUSTOTAL", 
                "malicious": malicious_count, 
                "suspicious": suspicious_count,
                "scan_date": attributes.get('last_analysis_date')
            }
            
        elif response.status_code == 404:
            ctx.logger.info("File unknown to VirusTotal.")
            return "UNKNOWN", {"source": "VIRUSTOTAL", "reason": "NOT_FOUND"}
        else:
            ctx.logger.warning(f"VirusTotal API Error: {response.status_code}")
            return "UNKNOWN", {"source": "VIRUSTOTAL", "reason": "API_ERROR"}
            
    except Exception as e:
        ctx.logger.error(f"Error contacting VirusTotal: {e}")
        return "UNKNOWN", {"source": "VIRUSTOTAL", "reason": "EXCEPTION"}

def handle_malicious_file(ctx, bucket, key, file_hash, details):
    """
    Orchestrates the Containment and Eradication of a malicious file.
    """
    log_nist_phase(ctx, PHASE_CONTAINMENT, f"Initiating response for threat: {file_hash}")
    
    # Alerting (Notification)
    alert_servicenow(ctx, {
        "hash": file_hash,
        "bucket": bucket,
        "key": key,
        "threat_details": details,
        "vector": VECTOR_MALICIOUS_CODE
    })
    
    # AD Action (Containment)
    disable_ad_user(ctx, "unknown_user") # Placeholder as user is not always in event
    
    # Deletion (Eradication)
    log_nist_phase(ctx, PHASE_ERADICATION, f"Deleting malicious object: s3://{bucket}/{key}")
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        ctx.logger.info("✅ File successfully deleted.")
        log_nist_phase(ctx, PHASE_RECOVERY, "System returned to clean state.")
    except Exception as e:
        ctx.logger.critical(f"❌ FAILED TO DELETE MALICIOUS FILE: {e}")
        log_nist_phase(ctx, PHASE_ERADICATION, "ERADICATION FAILED - MANUAL INTERVENTION REQUIRED")

def alert_servicenow(ctx, metadata):
    """Stub for sending security alerts."""
    url = os.environ.get('SERVICENOW_URL')
    if not url:
        ctx.logger.info(f"📢 [STUB] ServiceNow Alert: Found Malware {metadata['hash']}. (No URL configured)")
        return
        
    ctx.logger.info(f"📢 [STUB] POST {url} - Alerting Security Team about {metadata['hash']}")
    # Real implementation would utilize requests.post(url, json=metadata)

def disable_ad_user(ctx, user):
    """Stub for disabling a compromised user account."""
    ctx.logger.info(f"🔒 [STUB] Active Directory: Disabling account for user '{user}' (Containment Action)")

def log_nist_phase(ctx, phase, message):
    """Helper to enforce consistent NIST 800-61R3 logging format."""
    ctx.logger.info(f"🛡️ [NIST|{phase}] {message}")
