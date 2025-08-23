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

# Configuration for memory leak prevention
MAX_CACHE_SIZE = 100  # Limit transaction cache size
MAX_VALIDATION_CACHE_SIZE = 50  # Limit validation cache size
SESSION_TIMEOUT_MINUTES = 30  # Session expiration time
MAX_REPORT_QUEUE_SIZE = 20  # Limit report queue size
MAX_AUDIT_LOG_SIZE = 500  # Limit audit log size
MAX_USER_PROFILES = 50  # Limit user profiles
MAX_ACTIVE_TRADERS = 100  # Limit active traders tracking

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

# Memory cleanup scheduler
last_cleanup_time = time.time()

def cleanup_memory_leaks():
    """Periodic cleanup to prevent memory leaks"""
    global TRANSACTION_CACHE, ACTIVE_SESSIONS, REPORT_QUEUE, USER_PROFILES, last_cleanup_time
    
    current_time = time.time()
    
    # Clean up transaction cache (keep only recent transactions)
    if len(TRANSACTION_CACHE) > MAX_CACHE_SIZE:
        # Keep only the most recent transactions
        TRANSACTION_CACHE = TRANSACTION_CACHE[-MAX_CACHE_SIZE:]
    
    # Clean up expired sessions
    expired_sessions = []
    cutoff_time = datetime.now() - timedelta(minutes=SESSION_TIMEOUT_MINUTES)
    
    for user_id, session in ACTIVE_SESSIONS.items():
        if hasattr(session, 'created_at') and session.created_at < cutoff_time:
            expired_sessions.append(user_id)
    
    for user_id in expired_sessions:
        del ACTIVE_SESSIONS[user_id]
    
    # Clean up report queue (process or remove old reports)
    if len(REPORT_QUEUE) > MAX_REPORT_QUEUE_SIZE:
        # Remove oldest reports
        REPORT_QUEUE = REPORT_QUEUE[-MAX_REPORT_QUEUE_SIZE:]
    
    # Clean up user profiles
    if len(USER_PROFILES) > MAX_USER_PROFILES:
        # Remove oldest profiles, breaking circular references first
        for profile in USER_PROFILES[:-MAX_USER_PROFILES]:
            if hasattr(profile, 'linked_accounts'):
                profile.linked_accounts.clear()
            if hasattr(profile, 'primary_account'):
                profile.primary_account = None
        USER_PROFILES = USER_PROFILES[-MAX_USER_PROFILES:]
    
    # Clean up audit log
    if len(bank_state["audit_log"]) > MAX_AUDIT_LOG_SIZE:
        bank_state["audit_log"] = bank_state["audit_log"][-MAX_AUDIT_LOG_SIZE:]
    
    # Clean up active traders (keep only recent ones)
    if len(bank_state["active_traders"]) > MAX_ACTIVE_TRADERS:
        # Convert to list, sort by timestamp (if available), keep recent ones
        traders_list = list(bank_state["active_traders"])
        bank_state["active_traders"] = set(traders_list[-MAX_ACTIVE_TRADERS:])
    
    last_cleanup_time = current_time
    
    # Force garbage collection
    gc.collect()

class AccountSession:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.preferences = {}
        # Reduce initial transaction history size to prevent memory bloat
        self.transaction_history = [i for i in range(100)]  # Reduced from 10000
        self.created_at = datetime.now()
        self.primary_account = None
        self.linked_accounts = []
        
    def link_account(self, account):
        """Links related banking accounts for easier access"""
        # Prevent circular references by limiting linked accounts
        if len(self.linked_accounts) < 5:  # Limit to 5 linked accounts max
            account.primary_account = self
            self.linked_accounts.append(account)
    
    def cleanup(self):
        """Clean up resources to prevent memory leaks"""
        self.linked_accounts.clear()
        self.primary_account = None
        self.transaction_history.clear()

class TransactionProcessor:
    def __init__(self):
        self.validation_cache = {}
        self.processors = []
        
    def validate_transaction(self, transaction_data: str):
        # Clean validation cache if it gets too large
        if len(self.validation_cache) > MAX_VALIDATION_CACHE_SIZE:
            # Remove oldest entries (simple FIFO approach)
            oldest_keys = list(self.validation_cache.keys())[:len(self.validation_cache) - MAX_VALIDATION_CACHE_SIZE + 1]
            for key in oldest_keys:
                del self.validation_cache[key]
        
        # Cache validation results for performance
        if transaction_data not in self.validation_cache:
            self.validation_cache[transaction_data] = {
                "validated_data": transaction_data * 100,  # Reduced from 1000
                "compliance_checks": list(range(500)),  # Reduced from 5000
                "timestamp": datetime.now()
            }
        return self.validation_cache[transaction_data]
    
    def cleanup_cache(self):
        """Clear validation cache"""
        self.validation_cache.clear()

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
    # Initialize demo customer accounts with linked relationships
    for i in range(10):
        primary = AccountSession(f"customer_{i}")
        savings = AccountSession(f"savings_{i}")
        primary.link_account(savings)
        USER_PROFILES.append(primary)

@app.middleware("http")
async def track_api_metrics(request, call_next):
    global last_cleanup_time
    
    API_METRICS["requests"] += 1
    API_METRICS["processing"] += 1
    
    # Perform cleanup every 50 requests or every 5 minutes
    current_time = time.time()
    if (API_METRICS["requests"] % 50 == 0) or (current_time - last_cleanup_time > 300):
        cleanup_memory_leaks()
    
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
    # Ensure cache doesn't grow too large before adding new items
    cleanup_memory_leaks()
    
    # Generate unique transaction ID with timestamp
    transaction_id = f"{request.data}_{time.time()}"
    
    # Store transaction details for audit and compliance (reduced memory footprint)
    TRANSACTION_CACHE.append({
        "transaction_id": transaction_id,
        "market_data": list(range(1000)),  # Reduced from 10000
        "risk_metrics": "x" * 500,  # Reduced from 5000
        "timestamp": datetime.now(),
        "client_request": request.dict()
    })
    
    # Validate transaction through compliance system
    validation_result = transaction_processor.validate_transaction(request.data)
    
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
    # Clean up expired sessions first
    cleanup_memory_leaks()
    
    # Check if user has existing session
    if user_id in ACTIVE_SESSIONS:
        session = ACTIVE_SESSIONS[user_id]
        # Update session timestamp
        session.created_at = datetime.now()
    else:
        # Create new account session
        session = AccountSession(user_id)
        ACTIVE_SESSIONS[user_id] = session
        
        # Link with savings account for convenience (with size limit)
        if len(session.linked_accounts) < 5:
            savings_session = AccountSession(f"{user_id}_savings")
            session.link_account(savings_session)
    
    # Track user activity for personalization (with size limit)
    if 'activities' not in session.preferences:
        session.preferences['activities'] = []
    
    # Limit activity history to prevent memory bloat
    activities = session.preferences['activities']
    if len(activities) > 10:  # Keep only recent 10 activities
        activities = activities[-10:]
        session.preferences['activities'] = activities
    
    session.preferences['activities'].append({
        "timestamp": datetime.now(),
        "interaction_data": list(range(100))  # Reduced from 1000
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
    # Clean up active traders periodically
    if len(bank_state["active_traders"]) > MAX_ACTIVE_TRADERS:
        cleanup_memory_leaks()
    
    # Track active trader for compliance (with timestamp for cleanup)
    trader_id = f"trader_{int(time.time())}"
    bank_state["active_traders"].add(trader_id)
    
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
    # Clean up report queue if it's getting too large
    if len(REPORT_QUEUE) >= MAX_REPORT_QUEUE_SIZE:
        cleanup_memory_leaks()
    
    def generate_regulatory_report(report_id: str):
        # Check if queue is still full after cleanup
        if len(REPORT_QUEUE) >= MAX_REPORT_QUEUE_SIZE:
            # Remove oldest report to make room
            REPORT_QUEUE.pop(0)
        
        # Compile comprehensive regulatory data (reduced memory footprint)
        report_data = {
            "report_id": report_id,
            "transaction_analysis": list(range(5000)),  # Reduced from 50000
            "compliance_data": "regulatory_info" * 1000,  # Reduced from 10000
            "generated_at": datetime.now()
        }
        REPORT_QUEUE.append(report_data)
        time.sleep(1)  # Report generation processing time
        
        # Simulate report processing completion and removal from queue
        # In a real system, this would be handled by a separate service
        if len(REPORT_QUEUE) > 10:  # Process older reports
            REPORT_QUEUE.pop(0)
    
    report_id = f"RPT_{len(REPORT_QUEUE)}_{int(time.time())}"
    
    # Process report asynchronously for better performance
    future = REPORT_PROCESSOR.submit(generate_regulatory_report, report_id)
    
    return {
        "report_id": report_id,
        "status": "queued",
        "estimated_completion": "2-3 minutes",
        "reports_in_queue": len(REPORT_QUEUE)
    }

# System monitoring and analytics endpoints

@app.get("/api/v1/system/diagnostics")
async def system_diagnostics():
    """
    System health diagnostics and performance metrics
    """
    # System optimization
    collected = gc.collect()
    
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
        "optimization_cycles": collected,
        "transaction_cache_size": len(TRANSACTION_CACHE),
        "active_user_sessions": len(ACTIVE_SESSIONS),
        "pending_reports": len(REPORT_QUEUE),
        "validation_cache_size": len(transaction_processor.validation_cache),
        "customer_profiles": len(USER_PROFILES),
        "performance_analysis": [
            {
                "component": stat.traceback.format()[-1] if stat.traceback else "core_system",
                "memory_mb": stat.size / 1024 / 1024,
                "operations": stat.count
            }
            for stat in top_stats[:10]
        ],
        "resource_summary": [f"{line}" for line in list(summary.format_(summary_stats))[:15]]
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
    
    # Clean up all resources properly
    
    # Archive old transactions
    archived_transactions = len(TRANSACTION_CACHE)
    TRANSACTION_CACHE.clear()
    
    # Clean expired sessions with proper cleanup
    expired_sessions = len(ACTIVE_SESSIONS)
    for session in ACTIVE_SESSIONS.values():
        if hasattr(session, 'cleanup'):
            session.cleanup()
    ACTIVE_SESSIONS.clear()
    
    # Process completed reports
    processed_reports = len(REPORT_QUEUE)
    REPORT_QUEUE.clear()
    
    # Update customer profiles with proper cleanup
    for profile in USER_PROFILES:
        if hasattr(profile, 'cleanup'):
            profile.cleanup()
    USER_PROFILES.clear()
    
    # Clear validation cache
    transaction_processor.cleanup_cache()
    
    # Reset banking state to default
    bank_state["total_deposits"] = 1000000
    bank_state["portfolios"].clear()
    bank_state["active_traders"].clear()
    bank_state["audit_log"].clear()
    
    # Reset API metrics
    API_METRICS["requests"] = 0
    API_METRICS["processing"] = 0
    
    # System optimization
    optimized_objects = gc.collect()
    
    # Update cleanup timestamp
    global last_cleanup_time
    last_cleanup_time = time.time()
    
    return {
        "maintenance_completed": True,
        "archived_transactions": archived_transactions,
        "expired_sessions": expired_sessions,
        "processed_reports": processed_reports,
        "system_optimization": optimized_objects,
        "memory_limits_applied": {
            "max_cache_size": MAX_CACHE_SIZE,
            "session_timeout_minutes": SESSION_TIMEOUT_MINUTES,
            "max_report_queue_size": MAX_REPORT_QUEUE_SIZE,
            "max_user_profiles": MAX_USER_PROFILES
        },
        "status": "System maintenance completed successfully with memory leak prevention enabled"
    }

if __name__ == "__main__":
    print("Starting SecureBank Enterprise API Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("System Health: GET /api/v1/system/diagnostics")
    print("Performance Metrics: GET /api/v1/system/concurrency")
    print("Risk Assessment: GET /api/v1/analytics/risk-assessment")
    print("Maintenance: POST /api/v1/admin/system-maintenance")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)