# Generated by Django 2.0.5 on 2018-11-26 10:52

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('noma', '0002_auto_20181126_1756'),
    ]

    operations = [
        migrations.AlterField(
            model_name='nomasetact',
            name='tfunc',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='noma.NomaFunc'),
        ),
    ]