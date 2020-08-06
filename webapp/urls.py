from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('kafka', views.kafka_data, name='kafka_data'),
]