from django.shortcuts import render
from django.http import HttpResponse


def index(request):
    """
    This view if for show redis stats to user
    :param request:
    :return:
    """
    return HttpResponse("Hello, world. You're at the polls index.")