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

    <!-- Custom CSS -->
    <link rel="stylesheet" href="/static/css/main.css">
    <link rel="stylesheet" href="/static/css/components.css">
    <link rel="stylesheet" href="/static/css/dashboard.css">

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

    <!-- Custom JavaScript -->
    <script src="/static/js/main.js"></script>
    <script src="/static/js/utils.js"></script>
    <script src="/static/js/api.js"></script>

    {% block extra_js %}{% endblock %}
</body>
</html>
        '''

        # Enhanced Index template - now integrated into frontend manager
        self.templates['index'] = '''
{% extends "base.html" %}
{% block title %}Video Analytics Dashboard{% endblock %}

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
                        <i class="bi bi-clock"></i> <span data-duration="{{ video.duration_seconds }}"></span>
                    </div>
                    <div class="col-6">
                        <i class="bi bi-eye"></i> <span data-views="{{ video.view_count }}"></span>
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

<!-- Quick Compilation Panel (Hidden by default) -->
<div id="quickCompilationPanel" class="quick-compilation-panel">
    <div class="panel-content">
        <h6><i class="bi bi-collection"></i> Quick Compilation</h6>
        <div class="video-count">
            <span id="quickCompCount">0</span> videos selected
        </div>
        <div class="panel-actions">
            <button class="btn btn-primary btn-sm" onclick="createQuickCompilation()">
                <i class="bi bi-magic"></i> Create
            </button>
            <button class="btn btn-outline-secondary btn-sm" onclick="clearQuickCompilation()">
                <i class="bi bi-x"></i> Clear
            </button>
        </div>
    </div>
</div>
{% endblock %}

{% block extra_js %}
<script src="/static/js/dashboard.js"></script>
{% endblock %}
'''

        # Compilation Creation Interface with Advanced Form Controls
        self.templates['create_compilation'] = '''
{% extends "base.html" %}
{% block title %}Create New Compilation - Video Analytics{% endblock %}

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
<script src="/static/js/compilation-wizard.js"></script>
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
<script src="/static/js/user-compilations.js"></script>
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
'''
