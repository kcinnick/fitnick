from django.shortcuts import render


def index(request):
    index_context = {
        "index": 'index!',
    }

    return render(request, 'index.html', index_context)
