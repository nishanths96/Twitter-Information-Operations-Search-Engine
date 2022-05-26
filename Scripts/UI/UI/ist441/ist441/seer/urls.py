from django.urls import path
from . import views
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('home/', views.search_on_tweet_text, name='home'),
    # path('query/', views.query, name='query'),
    # path('search/', views.search, name='search'),
    path('display_tweet_bursts/', views.search_on_tweet_bursts, name = "display_tweet_bursts"),
    path('display_tweets/', views.search_on_tweet_text, name='display_tweet_text'),
    path('display_locations/', views.search_on_locations, name='display_location'),
    path('display_userids/', views.search_on_userid, name='display_userid'),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
