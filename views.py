from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.contrib.sessions.models import Session
from django.conf import settings
from dialogflow_lite.dialogflow import Dialogflow

import json

from django.views.generic import ListView
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import GenericViewSet

from django_dialogflow.models import ChatHistory
from django_dialogflow.serializers import ChatSerializer


def convert(data):
    if isinstance(data, bytes):
        return data.decode("utf-8", "strict")
    if isinstance(data, dict):
        return dict(map(convert, data.items()))
    if isinstance(data, tuple):
        return map(convert, data)

    return data


@require_http_methods(['POST'])
@method_decorator(csrf_exempt)
def chat_view(request):
    dialogflow = Dialogflow(**settings.DIALOGFLOW)
    input_dict = convert(request.body)
    input_text = json.loads(input_dict)['text']
    if not request.session.session_key:
        request.session.save()
    dialogflow.session_id = request.session.session_key

    if request.method == "GET":
        # Return a method not allowed response
        data = {
            'detail': 'You should make a POST request to this endpoint.',
            'name': '/chat'
        }
        return JsonResponse(data, status=405)
    elif request.method == "POST":
        data = chat_dialogflow(dialogflow, input_text)
        return JsonResponse(data, status=200)
    elif request.method == "PATCH":
        data = {
            'detail': 'You should make a POST request to this endpoint.',
            'name': '/chat'
        }

        # Return a method not allowed response
        return JsonResponse(data, status=405)

    elif request.method == "DELETE":
        data = {
            'detail': 'You should make a POST request to this endpoint.',
            'name': '/chat'
        }

        # Return a method not allowed response
        return JsonResponse(data, status=405)


class ChatHistoryListView(ListView):
    model = ChatHistory
    template_name = "chat_history.html"

    def get_context_data(self, *args, **kwargs):
        context = super(ChatHistoryListView, self).get_context_data(*args, **kwargs)
        result_dict = dict()
        for object in ChatHistory.objects.all().order_by('-time_stamp'):
            if object.session_id in result_dict:
                result_dict[object.session_id].append(object)
            else:
                result_dict[object.session_id] = [object]

        context['chat_history'] = result_dict
        return context


def chat_session_view(request):
    if not request.session.session_key:
        request.session.save()

    if request.method == "GET":
        # Return a method not allowed response
        data = {
            'session_id': request.session.session_key,
        }
        return JsonResponse(data, status=200)
    else:
        data = {
            'detail': 'You should make a POST request to this endpoint.',
            'name': '/chat_session_view'
        }

        # Return a method not allowed response
        return JsonResponse(data, status=405)


class ChatAPI(APIView):

    def get(self, request, format=None):
        data = {
            'detail': 'You should make a POST request to this endpoint.',
            'name': '/chat_api'
        }
        return Response(data, status=status.HTTP_400_BAD_REQUEST)

    def post(self, request, format=None):
        serializer = ChatSerializer(data=request.data)
        if serializer.is_valid():

            dialogflow = Dialogflow(**settings.DIALOGFLOW)
            input_text = serializer.data.get('text')
            dialogflow.session_id = serializer.data.get('php_session')

            return Response(chat_dialogflow(dialogflow, input_text), status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


def chat_dialogflow(dialogflow, input_text):
    responses = dialogflow.text_request(str(input_text))
    dialogflowData = dialogflow.query_response.get('result')

    dialogflowMessages = dialogflowData['fulfillment']['messages']
    ChatHistory.objects.create(chat_request=input_text,
                               chat_response=dialogflow.query_response.get("result"),
                               session_id=dialogflow.session_id
                               )
    return dict(text=dialogflowMessages, session_id=dialogflow.session_id)