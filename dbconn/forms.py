from django import forms
from .models import DBconn, History

class DBconnForm(forms.ModelForm):
    class Meta:
        model = DBconn
        fields = ('username', 'password', 'server_ip', 'port')

# class HistoryForm(forms.ModelForm):
#     class Meta:
#         model = History
#         field = 