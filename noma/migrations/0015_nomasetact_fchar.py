# Generated by Django 2.0.5 on 2018-12-14 00:30

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('noma', '0014_remove_nomasetact_tpar'),
    ]

    operations = [
        migrations.AddField(
            model_name='nomasetact',
            name='fchar',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
