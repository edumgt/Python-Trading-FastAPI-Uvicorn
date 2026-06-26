from os import getenv

from django.http import JsonResponse
from django.shortcuts import render


def _ctx(extra=None):
    ctx = {
        "api_base_url": getenv("FLASK_API_BASE_URL", "http://127.0.0.1:5000"),
        "airflow_ui_url": getenv("AIRFLOW_UI_URL", "http://127.0.0.1:8080"),
    }
    if extra:
        ctx.update(extra)
    return ctx


def home(request):
    return render(request, "dashboard/index.html", _ctx())


def login_page(request):
    return render(request, "dashboard/login.html", _ctx())


def signup_page(request):
    return render(request, "dashboard/signup.html", _ctx())


def users_page(request):
    return render(request, "dashboard/users.html", _ctx())


def health(request):
    return JsonResponse({"status": "ok", "service": "django-web"})
