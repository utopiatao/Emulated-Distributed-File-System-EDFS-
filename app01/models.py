from django.db import models

# Create your models here.
class UserInfo(models.Model):
    name = models.CharField(max_length=32)
    password = models.CharField(max_length=64)
    age = models.IntegerField()

class NameNode(models.Model):
    parent_path = models.CharField(max_length=256)
    curr_path = models.CharField(max_length=256)

class DataNode(models.Model):
    file_name = models.CharField(max_length=256)
    curr_path = models.CharField(max_length=256)
    partition_table_name = models.CharField(max_length=256)
    fields = models.CharField(max_length=256)