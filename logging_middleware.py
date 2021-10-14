from pinax.eventlog.models import log
from django.utils.deprecation import MiddlewareMixin

from accounts.models import Device


class RequestLoggerMiddleware(MiddlewareMixin):
    def process_request(self, request):
        extra = {"ip": self.get_client_ip(request)}
        if request.path == "/" or self.is_action_to_log(request.path):
            if request.method == 'POST':
                extra.update(request.POST)
            elif request.method == 'GET':
                extra.update(request.GET)
            if request.user.is_authenticated:
              log(user=request.user, action=request.path, extra=extra)
            elif request.user.is_authenticated:
              try:
                device = Device.objects.get(key=request.data.get('key'))
                log(user=device, action=request.path, extra=extra)
              except Device.DoesNotExist:
                log(user=request.user, action=request.path, extra=extra )

    def is_action_to_log(self, path):
        actions = ["attendance", "register"]
        for action in actions:
            if action in path:
                return True
        return False

    def get_client_ip(self, request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[-1].strip()
        else:
            ip = request.META.get('REMOTE_ADDR')
        return
