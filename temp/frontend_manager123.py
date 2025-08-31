from flask import render_template_string, jsonify, request
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json


class FrontendTemplateManager:
    """
    Advanced frontend template manager that generates dynamic, responsive HTML templates
    with sophisticated analytics dashboards, compilation creation interfaces, and 
    comprehensive data visualization components for the video compilation system.
    """

    def __init__(self):
        self.templates = {}
        self._initialize_templates()

    def _initialize_templates(self):
        """Initialize all template definitions with modern, responsive designs"""

        # Base template with advanced styling and interactive components
        self.templates['base'] = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}Video Compilation Analytics{% endblock %}</title>
    
    <!-- Bootstrap 5 with custom theme -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css">
    
    <!-- Chart.js for advanced analytics -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    
    <!-- Custom styling for enhanced UI/UX -->
    <style>
        :root {
            --primary-color: #2c3e50;
            --secondary-color: #3498db;
            --success-color: #27ae60;
            --warning-color: #f39c12;
            --danger-color: #e74c3c;
            --dark-bg: #1a1a1a;
            --card-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        body {
            background-color: #f8f9fa;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        .navbar-custom {
            background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .card-custom {
            border: none;
            border-radius: 12px;
            box-shadow: var(--card-shadow);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }
        
        .card-custom:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2);
        }
        
        .btn-custom {
            border-radius: 8px;
            font-weight: 500;
            padding: 0.5rem 1.5rem;
            transition: all 0.2s ease;
        }
        
        .stats-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 15px;
            padding: 1.5rem;
            margin-bottom: 1rem;
        }
        
        .stats-number {
            font-size: 2.5rem;
            font-weight: bold;
            display: block;
        }
        
        .loading-spinner {
            display: none;
            text-align: center;
            padding: 2rem;
        }
        
        .progress-custom {
            height: 8px;
            border-radius: 4px;
            background-color: #e9ecef;
            overflow: hidden;
        }
        
        .table-responsive-custom {
            border-radius: 12px;
            overflow: hidden;
            box-shadow: var(--card-shadow);
        }
        
        .status-badge {
            font-size: 0.75rem;
            padding: 0.4rem 0.8rem;
            border-radius: 20px;
            font-weight: 500;
        }
        
        .timeline-item {
            border-left: 3px solid var(--secondary-color);
            padding-left: 1rem;
            margin-bottom: 1rem;
            position: relative;
        }
        
        .timeline-item::before {
            content: '';
            position: absolute;
            left: -6px;
            top: 0.5rem;
            width: 10px;
            height: 10px;
            background: var(--secondary-color);
            border-radius: 50%;
        }
        
        @media (max-width: 768px) {
            .stats-number {
                font-size: 2rem;
            }
            .card-custom {
                margin-bottom: 1rem;
            }
        }
    </style>
    
    {% block extra_css %}{% endblock %}
</head>
<body>
    <!-- Advanced Navigation with Analytics Features -->
    <nav class="navbar navbar-expand-lg navbar-dark navbar-custom">
        <div class="container">
            <a class="navbar-brand fw-bold" href="/">
                <i class="bi bi-film me-2"></i>Video Analytics Pro
            </a>
            
            <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
                <span class="navbar-toggler-icon"></span>
            </button>
            
            <div class="collapse navbar-collapse" id="navbarNav">
                <ul class="navbar-nav me-auto">
                    <li class="nav-item">
                        <a class="nav-link" href="/">
                            <i class="bi bi-house me-1"></i>Videos
                        </a>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/compilations">
                            <i class="bi bi-collection-play me-1"></i>Compilations
                        </a>
                    </li>
                    <li class="nav-item dropdown">
                        <a class="nav-link dropdown-toggle" href="#" role="button" data-bs-toggle="dropdown">
                            <i class="bi bi-plus-circle me-1"></i>Create
                        </a>
                        <ul class="dropdown-menu">
                            <li><a class="dropdown-item" href="/create-compilation">
                                <i class="bi bi-magic me-2"></i>New Compilation
                            </a></li>
                            <li><a class="dropdown-item" href="/import">
                                <i class="bi bi-upload me-2"></i>Import Videos
                            </a></li>
                        </ul>
                    </li>
                    <li class="nav-item dropdown">
                        <a class="nav-link dropdown-toggle" href="#" role="button" data-bs-toggle="dropdown">
                            <i class="bi bi-graph-up me-1"></i>Analytics
                        </a>
                        <ul class="dropdown-menu">
                            <li><a class="dropdown-item" href="/stats">
                                <i class="bi bi-bar-chart me-2"></i>Overview Stats
                            </a></li>
                            <li><a class="dropdown-item" href="/video_usage_stats">
                                <i class="bi bi-diagram-3 me-2"></i>Usage Analytics
                            </a></li>
                            <li><a class="dropdown-item" href="/compilation-analytics">
                                <i class="bi bi-pie-chart me-2"></i>Compilation Analytics
                            </a></li>
                        </ul>
                    </li>
                    <li class="nav-item">
                        <a class="nav-link" href="/user-compilations">
                            <i class="bi bi-folder-open me-1"></i>My Compilations
                        </a>
                    </li>
                </ul>
                
                <!-- Quick Actions in Navigation -->
                <div class="d-flex">
                    <button class="btn btn-outline-light btn-sm me-2" onclick="processCompilations()">
                        <i class="bi bi-gear me-1"></i>Process
                    </button>
                    <button class="btn btn-outline-light btn-sm" onclick="updateStats()">
                        <i class="bi bi-arrow-clockwise me-1"></i>Refresh
                    </button>
                </div>
            </div>
        </div>
    </nav>
    
    <!-- Main Content Area -->
    <main class="container mt-4">
        <!-- Global Alerts Area -->
        <div id="alertContainer"></div>
        
        <!-- Loading Indicator -->
        <div id="loadingIndicator" class="loading-spinner">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
            <p class="mt-2">Processing request...</p>
        </div>
        
        {% block content %}{% endblock %}
    </main>
    
    <!-- Footer -->
    <footer class="mt-5 py-4 bg-dark text-light">
        <div class="container text-center">
            <p class="mb-0">&copy; 2024 Video Compilation Analytics. Advanced analytics for content creators.</p>
        </div>
    </footer>
    
    <!-- Bootstrap JS Bundle -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    
    <!-- Global JavaScript Functions -->
    <script>
        // Global utility functions for enhanced user experience
        function showAlert(message, type = 'info') {
            const alertContainer = document.getElementById('alertContainer');
            const alertDiv = document.createElement('div');
            alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
            alertDiv.innerHTML = `
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            `;
            alertContainer.appendChild(alertDiv);
            
            // Auto-dismiss after 5 seconds
            setTimeout(() => {
                if (alertDiv.parentNode) {
                    alertDiv.remove();
                }
            }, 6000);
        }
        
        function showLoading(show = true) {
            document.getElementById('loadingIndicator').style.display = show ? 'block' : 'none';
        }
        
        function processCompilations() {
            showLoading(true);
            fetch('/process_compilations', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    showLoading(false);
                    if (data.success) {
                        showAlert(`Processed ${data.processed} videos. Found ${data.new_compilations} new compilations.`, 'success');
                    } else {
                        showAlert(`Processing failed: ${data.error}`, 'danger');
                    }
                })
                .catch(error => {
                    showLoading(false);
                    showAlert('Processing request failed', 'danger');
                });
        }
        
        function updateStats() {
            showLoading(true);
            fetch('/update_usage_stats', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    showLoading(false);
                    if (data.success) {
                        showAlert('Statistics updated successfully', 'success');
                        location.reload();
                    } else {
                        showAlert(`Update failed: ${data.error}`, 'danger');
                    }
                })
                .catch(error => {
                    showLoading(false);
                    showAlert('Update request failed', 'danger');
                });
        }
        
        function formatNumber(num) {
            if (num >= 1000000) {
                return (num / 1000000).toFixed(1) + 'M';
            } else if (num >= 1000) {
                return (num / 1000).toFixed(1) + 'K';
            }
            return num.toString();
        }
        
        function formatDuration(seconds) {
            const hours = Math.floor(seconds / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            const secs = seconds % 60;
            
            if (hours > 0) {
                return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
            }
            return `${minutes}:${secs.toString().padStart(2, '0')}`;
        }
        
        // Advanced date formatting
        function formatDate(dateString) {
            const date = new Date(dateString);
            return date.toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }
    </script>
    
    {% block extra_js %}{% endblock %}
</body>
</html>
        '''

        # Compilation Creation Interface with Advanced Form Controls
        self.templates['create_compilation'] = '''
{% extends "base.html" %}
{% block title %}Create New Compilation - Video Analytics{% endblock %}

{% block extra_css %}
<style>
    .creation-wizard {
        background: white;
        border-radius: 15px;
        padding: 2rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
    }
    
    .wizard-step {
        display: none;
    }
    
    .wizard-step.active {
        display: block;
    }
    
    .step-indicator {
        display: flex;
        justify-content: center;
        margin-bottom: 2rem;
    }
    
    .step-indicator .step {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        background: #e9ecef;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 0 1rem;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .step-indicator .step.active {
        background: var(--secondary-color);
        color: white;
    }
    
    .step-indicator .step.completed {
        background: var(--success-color);
        color: white;
    }
    
    .duration-option {
        border: 2px solid #e9ecef;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem;
        cursor: pointer;
        transition: all 0.2s ease;
        text-align: center;
    }
    
    .duration-option:hover {
        border-color: var(--secondary-color);
        transform: translateY(-2px);
    }
    
    .duration-option.selected {
        border-color: var(--secondary-color);
        background-color: rgba(52, 152, 219, 0.1);
    }
    
    .preview-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
        border-left: 4px solid var(--secondary-color);
    }
    
    .category-breakdown {
        display: flex;
        justify-content: space-around;
        margin: 1rem 0;
    }
    
    .category-item {
        text-align: center;
        padding: 1rem;
        background: white;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        flex: 1;
        margin: 0 0.5rem;
    }
</style>
{% endblock %}

{% block content %}
<div class="row">
    <div class="col-md-10 offset-md-1">
        <div class="creation-wizard">
            <h2 class="text-center mb-4">
                <i class="bi bi-magic text-primary"></i>
                Create New Compilation
            </h2>
            
            <!-- Step Indicator -->
            <div class="step-indicator">
                <div class="step active" id="step1">1</div>
                <div class="step" id="step2">2</div>
                <div class="step" id="step3">3</div>
                <div class="step" id="step4">4</div>
            </div>
            
            <!-- Step 1: Duration Selection -->
            <div class="wizard-step active" id="wizardStep1">
                <h4 class="mb-4">Step 1: Choose Compilation Duration</h4>
                <p class="text-muted mb-4">Select the target duration for your compilation. The system will intelligently select videos to match this duration.</p>
                
                <div class="row" id="durationOptions">
                    <!-- Duration options will be loaded here -->
                </div>
                
                <div class="text-center mt-4">
                    <button class="btn btn-primary btn-lg" onclick="nextStep()" id="step1Next" disabled>
                        Next: Date Filter <i class="bi bi-arrow-right"></i>
                    </button>
                </div>
            </div>
            
            <!-- Step 2: Date Filter -->
            <div class="wizard-step" id="wizardStep2">
                <h4 class="mb-4">Step 2: Date Filter (Optional)</h4>
                <p class="text-muted mb-4">Choose videos published from a specific date onwards. Leave empty to include all videos.</p>
                
                <div class="row">
                    <div class="col-md-6">
                        <label for="fromDate" class="form-label">Videos published from:</label>
                        <input type="date" class="form-control form-control-lg" id="fromDate">
                        <div class="form-text">Optional: Filter videos by publication date</div>
                    </div>
                    <div class="col-md-6">
                        <label class="form-label">Quick Select:</label>
                        <div class="d-grid gap-2">
                            <button class="btn btn-outline-secondary" onclick="setQuickDate(30)">Last 30 Days</button>
                            <button class="btn btn-outline-secondary" onclick="setQuickDate(90)">Last 3 Months</button>
                            <button class="btn btn-outline-secondary" onclick="setQuickDate(180)">Last 6 Months</button>
                            <button class="btn btn-outline-secondary" onclick="setQuickDate(365)">Last Year</button>
                        </div>
                    </div>
                </div>
                
                <div class="text-center mt-4">
                    <button class="btn btn-secondary me-2" onclick="prevStep()">
                        <i class="bi bi-arrow-left"></i> Previous
                    </button>
                    <button class="btn btn-primary btn-lg" onclick="nextStep()">
                        Next: Preview <i class="bi bi-arrow-right"></i>
                    </button>
                </div>
            </div>
            
            <!-- Step 3: Preview and Analysis -->
            <div class="wizard-step" id="wizardStep3">
                <h4 class="mb-4">Step 3: Preview Compilation</h4>
                <p class="text-muted mb-4">Review the proposed compilation before creation.</p>
                
                <div id="previewContent">
                    <!-- Preview content will be loaded here -->
                </div>
                
                <div class="text-center mt-4">
                    <button class="btn btn-secondary me-2" onclick="prevStep()">
                        <i class="bi bi-arrow-left"></i> Previous
                    </button>
                    <button class="btn btn-success btn-lg" onclick="createCompilation()" id="createBtn">
                        <i class="bi bi-magic"></i> Create Compilation
                    </button>
                </div>
            </div>
            
            <!-- Step 4: Results -->
            <div class="wizard-step" id="wizardStep4">
                <div id="creationResults">
                    <!-- Results will be displayed here -->
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}

{% block extra_js %}
<script>
let currentStep = 1;
let selectedDuration = null;
let compilationPreview = null;

// Initialize the creation wizard
document.addEventListener('DOMContentLoaded', function() {
    loadDurationOptions();
});

function loadDurationOptions() {
    fetch('/api/available-durations')
        .then(response => response.json())
        .then(data => {
            const container = document.getElementById('durationOptions');
            container.innerHTML = '';
            
            data.durations.forEach(duration => {
                const optionDiv = document.createElement('div');
                optionDiv.className = 'col-md-3 col-sm-6';
                optionDiv.innerHTML = `
                    <div class="duration-option" onclick="selectDuration(${duration})">
                        <h5>${duration} Minutes</h5>
                        <p class="mb-0 text-muted">~${Math.floor(duration/5)} to ${Math.ceil(duration/3)} videos</p>
                    </div>
                `;
                container.appendChild(optionDiv);
            });
        })
        .catch(error => {
            showAlert('Failed to load duration options', 'danger');
        });
}

function selectDuration(duration) {
    selectedDuration = duration;
    
    // Update UI
    document.querySelectorAll('.duration-option').forEach(opt => {
        opt.classList.remove('selected');
    });
    event.currentTarget.classList.add('selected');
    
    // Enable next button
    document.getElementById('step1Next').disabled = false;
}

function setQuickDate(daysAgo) {
    const date = new Date();
    date.setDate(date.getDate() - daysAgo);
    document.getElementById('fromDate').value = date.toISOString().split('T')[0];
}

function nextStep() {
    if (currentStep === 2) {
        generatePreview();
    }
    
    // Hide current step
    document.getElementById(`wizardStep${currentStep}`).classList.remove('active');
    document.getElementById(`step${currentStep}`).classList.remove('active');
    document.getElementById(`step${currentStep}`).classList.add('completed');
    
    // Show next step
    currentStep++;
    document.getElementById(`wizardStep${currentStep}`).classList.add('active');
    document.getElementById(`step${currentStep}`).classList.add('active');
}

function prevStep() {
    // Hide current step
    document.getElementById(`wizardStep${currentStep}`).classList.remove('active');
    document.getElementById(`step${currentStep}`).classList.remove('active');
    
    // Show previous step
    currentStep--;
    document.getElementById(`wizardStep${currentStep}`).classList.add('active');
    document.getElementById(`step${currentStep}`).classList.remove('completed');
    document.getElementById(`step${currentStep}`).classList.add('active');
}

function generatePreview() {
    showLoading(true);
    
    const fromDate = document.getElementById('fromDate').value;
    
    fetch('/api/compilation-preview', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            duration: selectedDuration,
            from_date: fromDate
        })
    })
    .then(response => response.json())
    .then(data => {
        showLoading(false);
        if (data.success) {
            displayPreview(data);
            compilationPreview = data;
        } else {
            showAlert(`Preview generation failed: ${data.error}`, 'danger');
            prevStep(); // Go back to previous step
        }
    })
    .catch(error => {
        showLoading(false);
        showAlert('Preview generation failed', 'danger');
        prevStep();
    });
}

function displayPreview(previewData) {
    const container = document.getElementById('previewContent');
    
    const html = `
        <div class="preview-card">
            <h5><i class="bi bi-eye text-info"></i> Compilation Preview</h5>
            <div class="row">
                <div class="col-md-6">
                    <p><strong>Target Duration:</strong> ${selectedDuration} minutes</p>
                    <p><strong>Estimated Videos:</strong> ${previewData.estimated_video_count}</p>
                    <p><strong>Date Filter:</strong> ${previewData.from_date || 'None'}</p>
                </div>
                <div class="col-md-6">
                    <p><strong>Available Videos:</strong> ${previewData.total_available}</p>
                    <p><strong>Average Retention:</strong> ${previewData.avg_retention}%</p>
                    <p><strong>Quality Score:</strong> ${previewData.quality_score}/10</p>
                </div>
            </div>
        </div>
        
        <div class="category-breakdown">
            <div class="category-item">
                <h6>Top 25%</h6>
                <span class="stats-number text-success">${previewData.category_counts.top_25}</span>
                <small>High Quality</small>
            </div>
            <div class="category-item">
                <h6>Second 25%</h6>
                <span class="stats-number text-info">${previewData.category_counts.second_25}</span>
                <small>Good Quality</small>
            </div>
            <div class="category-item">
                <h6>Third 25%</h6>
                <span class="stats-number text-warning">${previewData.category_counts.third_25}</span>
                <small>Fair Quality</small>
            </div>
            <div class="category-item">
                <h6>Bottom 25%</h6>
                <span class="stats-number text-secondary">${previewData.category_counts.bottom_25}</span>
                <small>Lower Quality</small>
            </div>
        </div>
        
        <div class="alert alert-info">
            <i class="bi bi-info-circle"></i>
            <strong>Algorithm Notes:</strong> The first video will be selected from the highest quality category 
            to maximize viewer engagement. Subsequent videos will be intelligently mixed to maintain interest 
            while fitting the target duration.
        </div>
    `;
    
    container.innerHTML = html;
}

function createCompilation() {
    showLoading(true);
    document.getElementById('createBtn').disabled = true;
    
    const fromDate = document.getElementById('fromDate').value;
    
    fetch('/api/create-compilation', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            duration: selectedDuration,
            from_date: fromDate,
            title_prefix: 'Auto-Generated'
        })
    })
    .then(response => response.json())
    .then(data => {
        showLoading(false);
        displayResults(data);
        nextStep();
    })
    .catch(error => {
        showLoading(false);
        showAlert('Compilation creation failed', 'danger');
        document.getElementById('createBtn').disabled = false;
    });
}

function displayResults(results) {
    const container = document.getElementById('creationResults');
    
    if (results.success) {
        container.innerHTML = `
            <div class="text-center">
                <div class="mb-4">
                    <i class="bi bi-check-circle-fill text-success" style="font-size: 4rem;"></i>
                </div>
                <h3 class="text-success">Compilation Created Successfully!</h3>
                <p class="lead">Your new compilation is ready for review.</p>
                
                <div class="row mt-4">
                    <div class="col-md-6">
                        <div class="stats-card bg-success">
                            <span class="stats-number">${results.selected_videos_count}</span>
                            <span>Videos Selected</span>
                        </div>
                    </div>
                    <div class="col-md-6">
                        <div class="stats-card bg-info">
                            <span class="stats-number">${results.actual_duration_minutes}</span>
                            <span>Minutes Duration</span>
                        </div>
                    </div>
                </div>
                
                <div class="d-grid gap-2 d-md-flex justify-content-md-center mt-4">
                    <a href="/compilation-preview/${results.compilation_id}" class="btn btn-primary btn-lg">
                        <i class="bi bi-eye"></i> Preview Compilation
                    </a>
                    <a href="/user-compilations" class="btn btn-outline-primary btn-lg">
                        <i class="bi bi-list"></i> View All Compilations
                    </a>
                    <button class="btn btn-success btn-lg" onclick="location.reload()">
                        <i class="bi bi-plus"></i> Create Another
                    </button>
                </div>
            </div>
        `;
    } else {
        container.innerHTML = `
            <div class="text-center">
                <div class="mb-4">
                    <i class="bi bi-x-circle-fill text-danger" style="font-size: 4rem;"></i>
                </div>
                <h3 class="text-danger">Creation Failed</h3>
                <p class="lead">${results.error}</p>
                
                <div class="d-grid gap-2 d-md-flex justify-content-md-center mt-4">
                    <button class="btn btn-primary btn-lg" onclick="location.reload()">
                        <i class="bi bi-arrow-clockwise"></i> Try Again
                    </button>
                    <a href="/user-compilations" class="btn btn-outline-primary btn-lg">
                        <i class="bi bi-list"></i> View Existing Compilations
                    </a>
                </div>
            </div>
        `;
    }
}
</script>
{% endblock %}
        '''

        # User Compilations Dashboard with Management Features
        self.templates['user_compilations'] = '''
{% extends "base.html" %}
{% block title %}My Compilations - Video Analytics{% endblock %}

{% block content %}
<div class="d-flex justify-content-between align-items-center mb-4">
    <h2><i class="bi bi-folder-open text-primary"></i> My Compilations</h2>
    <div>
        <a href="/create-compilation" class="btn btn-primary">
            <i class="bi bi-plus-circle"></i> Create New
        </a>
        <button class="btn btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown">
            <i class="bi bi-filter"></i> Filter
        </button>
        <ul class="dropdown-menu">
            <li><a class="dropdown-item" href="?status=not_published">Not Published</a></li>
            <li><a class="dropdown-item" href="?status=published">Published</a></li>
            <li><a class="dropdown-item" href="?status=draft">Drafts</a></li>
            <li><hr class="dropdown-divider"></li>
            <li><a class="dropdown-item" href="/user-compilations">All Compilations</a></li>
        </ul>
    </div>
</div>

<!-- Statistics Overview -->
<div class="row mb-4">
    <div class="col-md-3">
        <div class="stats-card bg-primary">
            <span class="stats-number">{{ stats.total }}</span>
            <span>Total Compilations</span>
        </div>
    </div>
    <div class="col-md-3">
        <div class="stats-card bg-success">
            <span class="stats-number">{{ stats.published }}</span>
            <span>Published</span>
        </div>
    </div>
    <div class="col-md-3">
        <div class="stats-card bg-warning">
            <span class="stats-number">{{ stats.not_published }}</span>
            <span>Not Published</span>
        </div>
    </div>
    <div class="col-md-3">
        <div class="stats-card bg-info">
            <span class="stats-number">{{ stats.total_videos }}</span>
            <span>Total Videos</span>
        </div>
    </div>
</div>

<!-- Compilations Table -->
<div class="card-custom">
    <div class="card-body">
        <div class="table-responsive-custom">
            <table class="table table-hover">
                <thead class="table-dark">
                    <tr>
                        <th>Title</th>
                        <th>Duration</th>
                        <th>Videos</th>
                        <th>Status</th>
                        <th>Created</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
                    {% for compilation in compilations %}
                    <tr>
                        <td>
                            <strong>{{ compilation.title }}</strong>
                            {% if compilation.from_date_filter %}
                            <br><small class="text-muted">From: {{ compilation.from_date_filter }}</small>
                            {% endif %}
                        </td>
                        <td>
                            <span class="badge bg-info">{{ compilation.duration_rounded }} min</span>
                            <br><small class="text-muted">{{ "%.1f"|format(compilation.actual_duration_seconds / 60) }} actual</small>
                        </td>
                        <td>
                            <span class="badge bg-secondary">{{ compilation.video_count }} videos</span>
                        </td>
                        <td>
                            {% if compilation.status == 'published' %}
                                <span class="status-badge bg-success">Published</span>
                            {% elif compilation.status == 'not_published' %}
                                <span class="status-badge bg-warning text-dark">Not Published</span>
                            {% else %}
                                <span class="status-badge bg-secondary">Draft</span>
                            {% endif %}
                        </td>
                        <td>
                            <small>{{ compilation.created_at.strftime('%Y-%m-%d %H:%M') if compilation.created_at else 'Unknown' }}</small>
                        </td>
                        <td>
                            <div class="btn-group btn-group-sm">
                                <a href="/compilation-preview/{{ compilation._id }}" class="btn btn-outline-primary">
                                    <i class="bi bi-eye"></i>
                                </a>
                                {% if compilation.status != 'published' %}
                                <button class="btn btn-outline-success" onclick="publishCompilation('{{ compilation._id }}')">
                                    <i class="bi bi-cloud-upload"></i>
                                </button>
                                {% endif %}
                                <button class="btn btn-outline-info" onclick="exportCompilation('{{ compilation._id }}')">
                                    <i class="bi bi-download"></i>
                                </button>
                                {% if compilation.status != 'published' %}
                                <button class="btn btn-outline-danger" onclick="deleteCompilation('{{ compilation._id }}')">
                                    <i class="bi bi-trash"></i>
                                </button>
                                {% endif %}
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>

<!-- Pagination -->
{% if total_pages > 1 %}
<nav class="mt-4">
    <ul class="pagination justify-content-center">
        {% for page_num in range(1, total_pages + 1) %}
            {% if page_num == page %}
                <li class="page-item active">
                    <span class="page-link">{{ page_num }}</span>
                </li>
            {% else %}
                <li class="page-item">
                    <a class="page-link" href="?page={{ page_num }}{% if request.args.get('status') %}&status={{ request.args.get('status') }}{% endif %}">{{ page_num }}</a>
                </li>
            {% endif %}
        {% endfor %}
    </ul>
</nav>
{% endif %}
{% endblock %}

{% block extra_js %}
<script>
function publishCompilation(compilationId) {
    if (confirm('Are you sure you want to publish this compilation? This action cannot be undone.')) {
        fetch(`/api/compilation/${compilationId}/publish`, {
            method: 'POST'
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showAlert('Compilation published successfully', 'success');
                location.reload();
            } else {
                showAlert(`Publishing failed: ${data.error}`, 'danger');
            }
        })
        .catch(error => {
            showAlert('Publishing request failed', 'danger');
        });
    }
}

function exportCompilation(compilationId) {
    showLoading(true);
    
    fetch(`/api/compilation/${compilationId}/export`, {
        method: 'POST'
    })
    .then(response => response.json())
    .then(data => {
        showLoading(false);
        if (data.success) {
            showAlert(`Compilation exported successfully: ${data.filename}`, 'success');
            // Trigger download
            window.open(`/download-export/${data.filename}`, '_blank');
        } else {
            showAlert(`Export failed: ${data.error}`, 'danger');
        }
    })
    .catch(error => {
        showLoading(false);
        showAlert('Export request failed', 'danger');
    });
}

function deleteCompilation(compilationId) {
    if (confirm('Are you sure you want to delete this compilation? This action cannot be undone.')) {
        fetch(`/api/compilation/${compilationId}/delete`, {
            method: 'DELETE'
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showAlert('Compilation deleted successfully', 'success');
                location.reload();
            } else {
                showAlert(`Deletion failed: ${data.error}`, 'danger');
            }
        })
        .catch(error => {
            showAlert('Deletion request failed', 'danger');
        });
    }
}
</script>
{% endblock %}
        '''

        self.templates['index'] = '''
{% extends "base.html" %}
{% block title %}Video Analytics Dashboard{% endblock %}

{% block extra_css %}
<style>
    .video-card {
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        border-radius: 12px;
        overflow: hidden;
    }
    
    .video-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.15);
    }
    
    .video-thumbnail {
        width: 100%;
        height: 200px;
        object-fit: cover;
    }
    
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 1.5rem;
        margin-bottom: 2rem;
    }
    
    .filter-section {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 2rem;
    }
    
    .retention-badge {
        font-size: 0.8rem;
        padding: 0.3rem 0.6rem;
        border-radius: 15px;
        font-weight: 500;
    }
    
    .retention-high {
        background-color: #d4edda;
        color: #155724;
    }
    
    .retention-medium {
        background-color: #fff3cd;
        color: #856404;
    }
    
    .retention-low {
        background-color: #f8d7da;
        color: #721c24;
    }
</style>
{% endblock %}

{% block content %}
<!-- Quick Statistics Dashboard -->
<div class="stats-grid">
    <div class="stats-card bg-primary">
        <span class="stats-number">{{ quick_stats.total_videos }}</span>
        <span>Total Videos</span>
    </div>
    <div class="stats-card bg-success">
        <span class="stats-number">{{ quick_stats.compilations }}</span>
        <span>Compilations</span>
    </div>
    <div class="stats-card bg-info">
        <span class="stats-number">{{ quick_stats.high_retention }}</span>
        <span>High Retention</span>
    </div>
    <div class="stats-card bg-warning">
        <span class="stats-number">{{ quick_stats.user_compilations }}</span>
        <span>User Compilations</span>
    </div>
</div>

<!-- Advanced Filtering Section -->
<div class="filter-section">
    <form method="GET" class="row g-3">
        <div class="col-md-4">
            <label for="search" class="form-label">
                <i class="bi bi-search"></i> Search Videos
            </label>
            <input type="text" class="form-control" id="search" name="search" 
                  value="{{ search_query }}" placeholder="Search titles, descriptions...">
        </div>
        
        <div class="col-md-2">
            <label for="actor" class="form-label">
                <i class="bi bi-person"></i> Actor
            </label>
            <select class="form-select" id="actor" name="actor">
                <option value="">All Videos</option>
                <option value="true" {% if actor_filter == 'true' %}selected{% endif %}>With Actor</option>
                <option value="false" {% if actor_filter == 'false' %}selected{% endif %}>No Actor</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <label for="compilation" class="form-label">
                <i class="bi bi-collection"></i> Type
            </label>
            <select class="form-select" id="compilation" name="compilation">
                <option value="">All Types</option>
                <option value="true" {% if compilation_filter == 'true' %}selected{% endif %}>Compilations</option>
                <option value="false" {% if compilation_filter == 'false' %}selected{% endif %}>Single Videos</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <label for="retention" class="form-label">
                <i class="bi bi-graph-up"></i> Retention
            </label>
            <select class="form-select" id="retention" name="retention">
                <option value="">All Rates</option>
                <option value="high" {% if retention_filter == 'high' %}selected{% endif %}>High (70%+)</option>
                <option value="medium" {% if retention_filter == 'medium' %}selected{% endif %}>Medium (50-70%)</option>
                <option value="low" {% if retention_filter == 'low' %}selected{% endif %}>Low (<50%)</option>
            </select>
        </div>
        
        <div class="col-md-2">
            <label class="form-label">&nbsp;</label>
            <div class="d-grid">
                <button type="submit" class="btn btn-primary">
                    <i class="bi bi-funnel"></i> Filter
                </button>
            </div>
        </div>
    </form>
    
    {% if search_query or actor_filter or compilation_filter or retention_filter %}
    <div class="mt-3">
        <a href="/" class="btn btn-outline-secondary btn-sm">
            <i class="bi bi-x-circle"></i> Clear Filters
        </a>
        <span class="text-muted ms-2">Showing {{ total }} results</span>
    </div>
    {% endif %}
</div>

<!-- Videos Grid -->
<div class="row">
    {% for video in videos %}
    <div class="col-lg-4 col-md-6 mb-4">
        <div class="card video-card h-100">
            <img src="{{ video.thumbnail_url or 'https://via.placeholder.com/320x180?text=No+Thumbnail' }}" 
                class="video-thumbnail" alt="{{ video.title }}">
            
            <div class="card-body d-flex flex-column">
                <h6 class="card-title">{{ video.title[:80] }}{% if video.title|length > 80 %}...{% endif %}</h6>
                
                <div class="mb-2">
                    {% if video.is_compilation %}
                        <span class="badge bg-info me-1">
                            <i class="bi bi-collection"></i> Compilation
                        </span>
                    {% endif %}
                    
                    {% if video.actor %}
                        <span class="badge bg-success me-1">
                            <i class="bi bi-person"></i> Actor
                        </span>
                    {% endif %}
                    
                    <!-- Retention Rate Badge -->
                    {% set retention = video.average_view_percentage or 0 %}
                    {% if retention >= 70 %}
                        <span class="retention-badge retention-high">{{ "%.0f"|format(retention) }}% retention</span>
                    {% elif retention >= 50 %}
                        <span class="retention-badge retention-medium">{{ "%.0f"|format(retention) }}% retention</span>
                    {% else %}
                        <span class="retention-badge retention-low">{{ "%.0f"|format(retention) }}% retention</span>
                    {% endif %}
                </div>
                
                <div class="row text-muted small mb-2">
                    <div class="col-6">
                        <i class="bi bi-clock"></i> {{ formatDuration(video.duration_seconds) }}
                    </div>
                    <div class="col-6">
                        <i class="bi bi-eye"></i> {{ formatNumber(video.view_count) }}
                    </div>
                </div>
                
                {% if video.user_compilation_usage and video.user_compilation_usage.total_inclusions > 0 %}
                <div class="text-muted small mb-2">
                    <i class="bi bi-recycle text-info"></i> Used in {{ video.user_compilation_usage.total_inclusions }} compilations
                </div>
                {% endif %}
                
                <div class="mt-auto">
                    <div class="btn-group w-100">
                        <a href="/video/{{ video.video_id }}" class="btn btn-outline-primary btn-sm">
                            <i class="bi bi-eye"></i> View
                        </a>
                        <a href="/edit/{{ video.video_id }}" class="btn btn-outline-secondary btn-sm">
                            <i class="bi bi-pencil"></i> Edit
                        </a>
                        <button class="btn btn-outline-info btn-sm" onclick="addToQuickCompilation('{{ video.video_id }}')">
                            <i class="bi bi-plus"></i>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>
    {% endfor %}
</div>

<!-- No Results Message -->
{% if not videos %}
<div class="text-center py-5">
    <div class="mb-4">
        <i class="bi bi-inbox text-muted" style="font-size: 4rem;"></i>
    </div>
    <h4 class="text-muted">No videos found</h4>
    <p class="text-muted">Try adjusting your filters or search terms.</p>
    <a href="/" class="btn btn-primary">
        <i class="bi bi-arrow-clockwise"></i> Reset Filters
    </a>
</div>
{% endif %}

<!-- Pagination -->
{% if total_pages > 1 %}
<nav aria-label="Page navigation" class="mt-5">
    <ul class="pagination justify-content-center">
        {% if has_prev %}
            <li class="page-item">
                <a class="page-link" href="?page={{ page - 1 }}{% if search_query %}&search={{ search_query }}{% endif %}{% if actor_filter %}&actor={{ actor_filter }}{% endif %}{% if compilation_filter %}&compilation={{ compilation_filter }}{% endif %}{% if retention_filter %}&retention={{ retention_filter }}{% endif %}">
                    <i class="bi bi-chevron-left"></i> Previous
                </a>
            </li>
        {% endif %}
        
        {% for page_num in range(1, total_pages + 1) %}
            {% if page_num == page %}
                <li class="page-item active">
                    <span class="page-link">{{ page_num }}</span>
                </li>
            {% elif page_num == 1 or page_num == total_pages or (page_num >= page - 2 and page_num <= page + 2) %}
                <li class="page-item">
                    <a class="page-link" href="?page={{ page_num }}{% if search_query %}&search={{ search_query }}{% endif %}{% if actor_filter %}&actor={{ actor_filter }}{% endif %}{% if compilation_filter %}&compilation={{ compilation_filter }}{% endif %}{% if retention_filter %}&retention={{ retention_filter }}{% endif %}">{{ page_num }}</a>
                </li>
            {% elif page_num == page - 3 or page_num == page + 3 %}
                <li class="page-item disabled">
                    <span class="page-link">...</span>
                </li>
            {% endif %}
        {% endfor %}
        
        {% if has_next %}
            <li class="page-item">
                <a class="page-link" href="?page={{ page + 1 }}{% if search_query %}&search={{ search_query }}{% endif %}{% if actor_filter %}&actor={{ actor_filter }}{% endif %}{% if compilation_filter %}&compilation={{ compilation_filter }}{% endif %}{% if retention_filter %}&retention={{ retention_filter }}{% endif %}">
                    Next <i class="bi bi-chevron-right"></i>
                </a>
            </li>
        {% endif %}
    </ul>
</nav>
{% endif %}
{% endblock %}

{% block extra_js %}
<script>
// Quick compilation functionality
let quickCompilationVideos = [];

function addToQuickCompilation(videoId) {
    if (!quickCompilationVideos.includes(videoId)) {
        quickCompilationVideos.push(videoId);
        showAlert(`Added video to quick compilation. Total: ${quickCompilationVideos.length} videos`, 'success');
        
        // Show quick compilation panel if it has videos
        updateQuickCompilationPanel();
    } else {
        showAlert('Video already in quick compilation', 'warning');
    }
}

function updateQuickCompilationPanel() {
    // You can implement a floating panel for quick compilation
    console.log('Quick compilation videos:', quickCompilationVideos);
}

function formatDuration(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    
    if (hours > 0) {
        return `${hours}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
    }
    return `${minutes}:${secs.toString().padStart(2, '0')}`;
}

function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
}

// Auto-refresh functionality
let autoRefreshInterval;
function toggleAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
        showAlert('Auto-refresh disabled', 'info');
    } else {
        autoRefreshInterval = setInterval(() => {
            updateStats();
        }, 300000); // 5 minutes
        showAlert('Auto-refresh enabled (5 min intervals)', 'info');
    }
}
</script>
{% endblock %}
'''

    def get_template(self, template_name: str) -> str:
        """
        Retrieve a specific template with error handling and fallback options.
        
        Args:
            template_name: Name of the template to retrieve
            
        Returns:
            Template string or error template if not found
        """
        return self.templates.get(template_name, self._get_error_template(template_name))

    def _get_error_template(self, template_name: str) -> str:
      """Generate an error template when requested template is not found"""
      return '''
      {% extends "base.html" %}
      {% block title %}Template Error{% endblock %}
      {% block content %}
      <div class="alert alert-danger">
          <h4>Template Not Found</h4>
          <p>The template "''' + template_name + '''" could not be found.</p>
          <a href="/" class="btn btn-primary">Return Home</a>
      </div>
      {% endblock %}
      '''

    def render_template(self, template_name: str, **context) -> str:
        """
        Render a template with provided context data and enhanced error handling.
        
        Args:
            template_name: Name of the template to render
            **context: Template context variables
            
        Returns:
            Rendered HTML string
        """
        template_content = self.get_template(template_name)
        try:
            return render_template_string(template_content, **context)
        except Exception as e:
            # Return error template with debug information
            error_template = '''
            {% extends "base.html" %}
            {% block title %}Rendering Error{% endblock %}
            {% block content %}
            <div class="alert alert-danger">
                <h4>Template Rendering Error</h4>
                <p>{{ error_message }}</p>
                <details>
                    <summary>Technical Details</summary>
                    <pre>{{ error_details }}</pre>
                </details>
                <a href="/" class="btn btn-primary">Return Home</a>
            </div>
            {% endblock %}
            '''
            return render_template_string(
                error_template,
                error_message=f"Failed to render template '{template_name}'",
                error_details=str(e)
            )
