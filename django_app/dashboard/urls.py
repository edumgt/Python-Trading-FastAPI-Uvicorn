from django.urls import path

from .views import health, home, login_page, signup_page, users_page


urlpatterns = [
    path("", home, name="home"),
    path("health", health, name="health"),
    path("login", login_page, name="login"),
    path("signup", signup_page, name="signup"),
    path("users", users_page, name="users"),
]
