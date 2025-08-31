# Templates Directory

This directory should contain your Flask HTML templates.

Required template files:
- base.html
- index.html
- video_detail.html
- edit_video.html
- import.html
- stats.html
- compilations.html
- compilation_detail.html
- video_usage_stats.html
- video_usage_detail.html

You can create these templates based on your existing ones, or create new ones 
following Flask templating conventions.

Example base template structure:
```html
<!DOCTYPE html>
<html>
<head>
    <title>Video Analytics</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="/">Video Analytics</a>
            <div class="navbar-nav">
                <a class="nav-link" href="/">Videos</a>
                <a class="nav-link" href="/compilations">Compilations</a>
                <a class="nav-link" href="/stats">Stats</a>
                <a class="nav-link" href="/video_usage_stats">Usage Stats</a>
            </div>
        </div>
    </nav>
    
    <div class="container mt-4">
        {% block content %}{% endblock %}
    </div>
</body>
</html>
```
