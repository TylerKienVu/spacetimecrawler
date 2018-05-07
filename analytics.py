import pickle
from collections import defaultdict

with(open('mostOutLinksPage.file', 'rb')) as f:
    mostOutLinksPage = pickle.load(f)

with(open('validUrls.file', 'rb')) as f:
    validUrls = pickle.load(f)

print(mostOutLinksPage)
print(validUrls)
