"""
Enterprise Banking API Server
Secure banking operations with transaction processing and account management.
"""

import asyncio
import gc
import threading
import time
import tracemalloc
import psutil
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import sys

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel
from pympler import muppy, summary, tracker

# Memory leak fixes: Add size limits and cleanup mechanisms
MAX_TRANSACTION_CACHE = 100  # Limit cache size
MAX_VALIDATION_CACHE = 50    # Limit validation cache
SESSION_TIMEOUT_MINUTES = 30  # Session timeout
MAX_REPORT_QUEUE = 20        # Limit report queue

TRANSACTION_CACHE = []  
ACTIVE_SESSIONS = {} 
API_METRICS = {"requests": 0, "processing": 0}  
REPORT_QUEUE = [] 
USER_PROFILES = []   
REPORT_PROCESSOR = ThreadPoolExecutor(max_workers=4)

bank_state = {
    "total_deposits": 1000000,
    "portfolios": {"AAPL": 100, "GOOGL": 50, "MSFT": 75},
    "active_traders": set(),
    "audit_log": []
}

# Performance monitoring
performance_tracker = tracker.SummaryTracker()

# Memory management utility functions
def cleanup_expired_sessions():
    """Remove expired user sessions to prevent memory leaks"""
    current_time = datetime.now()
    expired_sessions = []
    
    for user_id, session in ACTIVE_SESSIONS.items():
        if hasattr(session, 'created_at'):
            age = current_time - session.created_at
            if age > timedelta(minutes=SESSION_TIMEOUT_MINUTES):
                expired_sessions.append(user_id)
    
    for user_id in expired_sessions:
        session = ACTIVE_SESSIONS.pop(user_id, None)
        if session and hasattr(session, 'linked_accounts'):
            # Break circular references
            session.linked_accounts.clear()
            if hasattr(session, 'primary_account'):
                session.primary_account = None
    
    return len(expired_sessions)

def cleanup_transaction_cache():
    """Limit transaction cache size to prevent unbounded growth"""
    if len(TRANSACTION_CACHE) > MAX_TRANSACTION_CACHE:
        # Keep only the most recent transactions
        excess_count = len(TRANSACTION_CACHE) - MAX_TRANSACTION_CACHE
        TRANSACTION_CACHE[:excess_count] = []
        return excess_count
    return 0

def cleanup_report_queue():
    """Process and remove completed reports to prevent queue buildup"""
    if len(REPORT_QUEUE) > MAX_REPORT_QUEUE:
        # Remove oldest reports
        excess_count = len(REPORT_QUEUE) - MAX_REPORT_QUEUE
        REPORT_QUEUE[:excess_count] = []
        return excess_count
    return 0

def perform_memory_cleanup():
    """Comprehensive memory cleanup to prevent leaks"""
    cleanup_stats = {
        "expired_sessions": cleanup_expired_sessions(),
        "excess_transactions": cleanup_transaction_cache(),
        "excess_reports": cleanup_report_queue(),
        "gc_collected": gc.collect()
    }
    return cleanup_stats

class AccountSession:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.preferences = {}
        self.transaction_history = [i for i in range(100)]  # Reduced from 10000 to 100
        self.created_at = datetime.now()
        self.primary_account = None
        self.linked_accounts = []
        
    def link_account(self, account):
        """Links related banking accounts for easier access"""
        # Prevent circular references by using weak references approach
        if len(self.linked_accounts) < 5:  # Limit linked accounts
            self.linked_accounts.append(account.user_id)  # Store ID instead of object reference
    
    def cleanup_references(self):
        """Clean up references to prevent memory leaks"""
        self.linked_accounts.clear()
        self.primary_account = None
        if 'activities' in self.preferences:
            # Keep only recent activities
            activities = self.preferences['activities']
            self.preferences['activities'] = activities[-10:] if len(activities) > 10 else activities

class TransactionProcessor:
    def __init__(self):
        self.validation_cache = {}
        self.processors = []
        
    def validate_transaction(self, transaction_data: str):
        # Cache validation results for performance but with size limits
        if transaction_data not in self.validation_cache:
            # Cleanup cache if it gets too large
            if len(self.validation_cache) >= MAX_VALIDATION_CACHE:
                # Remove oldest entries (simplified LRU)
                oldest_keys = list(self.validation_cache.keys())[:10]
                for key in oldest_keys:
                    self.validation_cache.pop(key, None)
            
            self.validation_cache[transaction_data] = {
                "validated_data": transaction_data * 100,  # Reduced from 1000 to 100
                "compliance_checks": list(range(500)),  # Reduced from 5000 to 500
                "timestamp": datetime.now()
            }
        return self.validation_cache[transaction_data]
    
    def cleanup_expired_cache(self):
        """Remove expired validation cache entries"""
        current_time = datetime.now()
        expired_keys = []
        
        for key, cache_data in self.validation_cache.items():
            if 'timestamp' in cache_data:
                age = current_time - cache_data['timestamp']
                if age > timedelta(minutes=15):  # Cache expires after 15 minutes
                    expired_keys.append(key)
        
        for key in expired_keys:
            self.validation_cache.pop(key, None)
        
        return len(expired_keys)

transaction_processor = TransactionProcessor()

app = FastAPI(title="SecureBank API", version="2.1.3", description="Enterprise Banking Platform")

# Models
class ProcessRequest(BaseModel):
    data: str
    iterations: int = 1

class TransferRequest(BaseModel):
    amount: float
    from_account: str
    to_account: str

# Start memory profiling
tracemalloc.start()

@app.on_event("startup")
async def startup_event():
    print("Initializing SecureBank API...")
    # Initialize demo customer accounts with linked relationships (fixed circular refs)
    for i in range(5):  # Reduced from 10 to 5 to save memory
        primary = AccountSession(f"customer_{i}")
        savings = AccountSession(f"savings_{i}")
        primary.link_account(savings)  # Now stores ID instead of object reference
        USER_PROFILES.append(primary)

@app.middleware("http")
async def track_api_metrics(request, call_next):
    API_METRICS["requests"] += 1
    API_METRICS["processing"] += 1
    
    response = await call_next(request)
    
    API_METRICS["processing"] -= 1
    return response

@app.get("/")
async def health_check():
    return {"status": "operational", "service": "SecureBank API", "version": "2.1.3"}

@app.post("/api/v1/transactions/process")
async def process_transaction(request: ProcessRequest):
    """
    Process high-frequency trading transactions with caching for performance
    """
    # Perform periodic cleanup to prevent memory leaks
    if len(TRANSACTION_CACHE) > 0 and len(TRANSACTION_CACHE) % 20 == 0:
        perform_memory_cleanup()
    
    # Generate unique transaction ID with timestamp
    transaction_id = f"{request.data}_{time.time()}"
    
    # Store transaction details for audit and compliance (reduced size)
    TRANSACTION_CACHE.append({
        "transaction_id": transaction_id,
        "market_data": list(range(1000)),  # Reduced from 10000 to 1000
        "risk_metrics": "x" * 500,  # Reduced from 5000 to 500
        "timestamp": datetime.now(),
        "client_request": request.dict()
    })
    
    # Cleanup transaction cache if it gets too large
    cleanup_transaction_cache()
    
    # Validate transaction through compliance system
    validation_result = transaction_processor.validate_transaction(request.data)
    
    # Cleanup validation cache periodically
    if len(transaction_processor.validation_cache) > 10:
        transaction_processor.cleanup_expired_cache()
    
    return {
        "transaction_id": transaction_id,
        "status": "processed", 
        "cached_transactions": len(TRANSACTION_CACHE),
        "validation_cache_size": len(transaction_processor.validation_cache)
    }

@app.post("/api/v1/accounts/session")
async def create_user_session(user_id: str):
    """
    Create persistent user session for account management
    """
    # Perform session cleanup periodically
    if len(ACTIVE_SESSIONS) > 10:
        cleanup_expired_sessions()
    
    # Check if user has existing session
    if user_id in ACTIVE_SESSIONS:
        session = ACTIVE_SESSIONS[user_id]
    else:
        # Create new account session
        session = AccountSession(user_id)
        ACTIVE_SESSIONS[user_id] = session
        
        # Link with savings account for convenience (fixed circular reference)
        savings_session = AccountSession(f"{user_id}_savings")
        session.link_account(savings_session)
    
    # Track user activity for personalization (with limits)
    if 'activities' not in session.preferences:
        session.preferences['activities'] = []
    
    # Limit activity tracking to prevent memory bloat
    activities = session.preferences['activities']
    if len(activities) >= 10:
        activities.pop(0)  # Remove oldest activity
    
    activities.append({
        "timestamp": datetime.now(),
        "interaction_data": list(range(100))  # Reduced from 1000 to 100
    })
    
    return {
        "user_id": user_id,
        "session_created": True,
        "total_activities": len(session.preferences.get('activities', [])),
        "active_sessions": len(ACTIVE_SESSIONS)
    }

@app.post("/api/v1/transfers/execute")
async def execute_transfer(request: TransferRequest):
    """
    Execute secure bank transfer between accounts
    """
    # Fraud detection processing
    await asyncio.sleep(0.01)
    
    # Check available funds
    current_deposits = bank_state["total_deposits"]
    
    # Additional compliance checks
    await asyncio.sleep(0.005)
    
    if current_deposits >= request.amount:
        # Process the transfer
        bank_state["total_deposits"] = current_deposits - request.amount
        
        # Record for audit compliance
        bank_state["audit_log"].append({
            "transaction_type": "wire_transfer",
            "amount": request.amount,
            "from_account": request.from_account,
            "to_account": request.to_account,
            "timestamp": datetime.now(),
            "remaining_deposits": bank_state["total_deposits"]
        })
        
        return {
            "status": "completed",
            "transfer_amount": request.amount,
            "confirmation_id": f"TX{int(time.time())}",
            "remaining_balance": bank_state["total_deposits"]
        }
    else:
        return {
            "status": "declined", 
            "reason": "insufficient_funds",
            "available_balance": bank_state["total_deposits"]
        }

@app.post("/api/v1/portfolio/update")
async def update_portfolio(item: str, quantity: int):
    """
    Update customer investment portfolio holdings
    """
    # Track active trader for compliance
    bank_state["active_traders"].add(f"trader_{time.time()}")
    
    # Initialize portfolio position if new
    if item not in bank_state["portfolios"]:
        bank_state["portfolios"][item] = 0
    
    # Market data processing delay
    await asyncio.sleep(0.01)
    
    current_holdings = bank_state["portfolios"][item]
    
    # Settlement processing time
    await asyncio.sleep(0.005)
    
    # Update portfolio holdings
    bank_state["portfolios"][item] = current_holdings + quantity
    
    return {
        "symbol": item,
        "updated_holdings": bank_state["portfolios"][item],
        "portfolio_value": sum(bank_state["portfolios"].values()),
        "active_traders": len(bank_state["active_traders"])
    }

@app.post("/api/v1/reports/generate")
async def generate_report(background_tasks: BackgroundTasks):
    """
    Generate comprehensive financial reports for compliance
    """
    # Clean up report queue if it gets too large
    cleanup_report_queue()
    
    def generate_regulatory_report(report_id: str):
        # Compile comprehensive regulatory data (reduced size)
        report_data = {
            "report_id": report_id,
            "transaction_analysis": list(range(5000)),  # Reduced from 50000 to 5000
            "compliance_data": "regulatory_info" * 1000,  # Reduced from 10000 to 1000
            "generated_at": datetime.now()
        }
        
        # Prevent unbounded report queue growth
        if len(REPORT_QUEUE) < MAX_REPORT_QUEUE:
            REPORT_QUEUE.append(report_data)
        else:
            # Replace oldest report
            REPORT_QUEUE.pop(0)
            REPORT_QUEUE.append(report_data)
            
        time.sleep(0.5)  # Reduced processing time from 1s to 0.5s
        # Report remains queued for regulatory submission
    
    report_id = f"RPT_{len(REPORT_QUEUE)}_{int(time.time())}"
    
    # Process report asynchronously for better performance
    future = REPORT_PROCESSOR.submit(generate_regulatory_report, report_id)
    
    return {
        "report_id": report_id,
        "status": "queued",
        "estimated_completion": "30 seconds",  # Updated from 2-3 minutes
        "reports_in_queue": len(REPORT_QUEUE)
    }

# System monitoring and analytics endpoints

@app.get("/api/v1/system/diagnostics")
async def system_diagnostics():
    """
    System health diagnostics and performance metrics
    """
    # Perform comprehensive cleanup before diagnostics
    cleanup_stats = perform_memory_cleanup()
    
    # Performance metrics
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    # Application profiling
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')
    
    # Resource utilization analysis
    all_objects = muppy.get_objects()
    summary_stats = summary.summarize(all_objects)
    
    return {
        "timestamp": datetime.now().isoformat(),
        "system_status": "operational",
        "memory_usage_mb": memory_info.rss / 1024 / 1024,
        "virtual_memory_mb": memory_info.vms / 1024 / 1024,
        "optimization_cycles": cleanup_stats["gc_collected"],
        "transaction_cache_size": len(TRANSACTION_CACHE),
        "active_user_sessions": len(ACTIVE_SESSIONS),
        "pending_reports": len(REPORT_QUEUE),
        "validation_cache_size": len(transaction_processor.validation_cache),
        "customer_profiles": len(USER_PROFILES),
        "memory_cleanup_stats": cleanup_stats,
        "performance_analysis": [
            {
                "component": stat.traceback.format()[-1] if stat.traceback else "core_system",
                "memory_mb": stat.size / 1024 / 1024,
                "operations": stat.count
            }
            for stat in top_stats[:10]
        ],
        "resource_summary": [f"{line}" for line in list(summary.format_(summary_stats))[:15]],
        "cache_limits": {
            "max_transaction_cache": MAX_TRANSACTION_CACHE,
            "max_validation_cache": MAX_VALIDATION_CACHE,
            "session_timeout_minutes": SESSION_TIMEOUT_MINUTES,
            "max_report_queue": MAX_REPORT_QUEUE
        }
    }

@app.get("/api/v1/system/concurrency")
async def concurrency_metrics():
    """
    Concurrency and threading performance metrics
    """
    worker_threads = []
    
    # Analyze worker thread performance
    for thread_id, frame in sys._current_frames().items():
        worker_info = {
            "worker_id": thread_id,
            "worker_name": threading.current_thread().name,
            "execution_stack": []
        }
        
        # Capture execution context
        current_frame = frame
        while current_frame:
            worker_info["execution_stack"].append({
                "module": current_frame.f_code.co_filename,
                "function": current_frame.f_code.co_name,
                "line": current_frame.f_lineno
            })
            current_frame = current_frame.f_back
        
        worker_threads.append(worker_info)
    
    return {
        "timestamp": datetime.now().isoformat(),
        "concurrent_workers": threading.active_count(),
        "banking_system_state": {
            "total_deposits": bank_state["total_deposits"],
            "portfolio_positions": len(bank_state["portfolios"]),
            "active_traders": len(bank_state["active_traders"]),
            "audit_entries": len(bank_state["audit_log"])
        },
        "api_metrics": API_METRICS.copy(),
        "report_processors": REPORT_PROCESSOR._threads,
        "worker_analysis": worker_threads[:5]  # Performance optimization
    }

@app.get("/api/v1/analytics/risk-assessment")
async def risk_assessment():
    """
    Comprehensive risk analysis and system assessment
    """
    # Analyze account relationships
    linked_account_count = 0
    for profile in USER_PROFILES:
        if hasattr(profile, 'primary_account') and profile.primary_account is not None:
            linked_account_count += 1
    
    # Market risk indicators
    market_risk_metrics = {
        "wire_transfers": len([log for log in bank_state["audit_log"] if log.get("transaction_type") == "wire_transfer"]),
        "portfolio_positions": len(bank_state["portfolios"]),
        "concurrent_transactions": API_METRICS["processing"]
    }
    
    return {
        "timestamp": datetime.now().isoformat(),
        "risk_analysis": {
            "transaction_cache_size_mb": sum(sys.getsizeof(item) for item in TRANSACTION_CACHE) / 1024 / 1024,
            "active_sessions_mb": sum(sys.getsizeof(session) for session in ACTIVE_SESSIONS.values()) / 1024 / 1024,
            "report_queue_mb": sum(sys.getsizeof(report) for report in REPORT_QUEUE) / 1024 / 1024,
            "linked_accounts": linked_account_count,
            "validation_cache_mb": sys.getsizeof(transaction_processor.validation_cache) / 1024 / 1024
        },
        "market_risk_indicators": market_risk_metrics,
        "compliance_recommendations": [
            "Implement transaction caching policies",
            "Establish session timeout protocols",
            "Enable distributed state management",
            "Optimize report generation pipeline",
            "Enhance account linking security",
            "Deploy atomic transaction processing"
        ]
    }

@app.post("/api/v1/admin/system-maintenance")
async def system_maintenance():
    """
    Perform system maintenance and optimization
    """
    global TRANSACTION_CACHE, ACTIVE_SESSIONS, REPORT_QUEUE, USER_PROFILES
    
    # Archive old transactions
    archived_transactions = len(TRANSACTION_CACHE)
    TRANSACTION_CACHE.clear()
    
    # Clean expired sessions
    expired_sessions = len(ACTIVE_SESSIONS)
    ACTIVE_SESSIONS.clear()
    
    # Process completed reports
    processed_reports = len(REPORT_QUEUE)
    REPORT_QUEUE.clear()
    
    # Update customer profiles
    for profile in USER_PROFILES:
        if hasattr(profile, 'primary_account'):
            profile.primary_account = None
        if hasattr(profile, 'linked_accounts'):
            profile.linked_accounts.clear()
    USER_PROFILES.clear()
    
    # Clear validation cache
    transaction_processor.validation_cache.clear()
    
    # Reset banking state to default
    bank_state["total_deposits"] = 1000000
    bank_state["portfolios"].clear()
    bank_state["active_traders"].clear()
    bank_state["audit_log"].clear()
    
    # System optimization
    optimized_objects = gc.collect()
    
    return {
        "maintenance_completed": True,
        "archived_transactions": archived_transactions,
        "expired_sessions": expired_sessions,
        "processed_reports": processed_reports,
        "system_optimization": optimized_objects,
        "status": "System maintenance completed successfully"
    }

if __name__ == "__main__":
    print("Starting SecureBank Enterprise API Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("System Health: GET /api/v1/system/diagnostics")
    print("Performance Metrics: GET /api/v1/system/concurrency")
    print("Risk Assessment: GET /api/v1/analytics/risk-assessment")
    print("Maintenance: POST /api/v1/admin/system-maintenance")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)