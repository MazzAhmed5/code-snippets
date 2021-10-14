from rest_framework import permissions


class ClosePartnerPermission(permissions.BasePermission):

    def has_object_permission(self, request, view, obj):
        user = request.user
        if not user.is_authenticated:
            return False
        if user.is_superuser or user.is_staff:
            return True
        if obj.users.filter(pk=user.id).exists():
            return True
        return False


class HasPartnerPermission(permissions.BasePermission):
    message = 'Invalid partner. Please provide a valid partner id.'

    def has_permission(self, request, view):
        partner_id = request.data.get('partner') or view.kwargs.get('partner_id') or request.GET.get('partner_id')
        if not partner_id:
            return False
        return request.user.partners.filter(id=partner_id).exists()

class HasSuperuserPermission(permissions.BasePermission):
    message = 'Only Works for superuser.'

    def has_permission(self, request, view):
        if request.user.is_superuser:
            return True
        else:
            return False 