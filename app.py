import argparse
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer

EVENT_TEMPLATES = [
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 100,
    "price": 3.69,
    "shipmentAddress": "541-428 Nulla Avenue",
    "zipCode": "4286"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 17,
    "price": 0.09,
    "shipmentAddress": "Ap #249-5876 Magna. Rd.",
    "zipCode": "I9E 0JN"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 16,
    "price": 7.76,
    "shipmentAddress": "525-8975 Urna. Street",
    "zipCode": "13965"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 185,
    "price": 5.77,
    "shipmentAddress": "473-8850 Tellus Street",
    "zipCode": "657101"
  },
  {
    "orderType": "E",
    "orderItemName": "Green Onion",
    "quantity": 84,
    "price": 5.17,
    "shipmentAddress": "Ap #535-7695 Fringilla Street",
    "zipCode": "70060"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 17,
    "price": 4.39,
    "shipmentAddress": "Ap #133-7694 Eleifend Ave",
    "zipCode": "239129"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 94,
    "price": 8.41,
    "shipmentAddress": "593-1014 Cras St.",
    "zipCode": "99-977"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 140,
    "price": 4.42,
    "shipmentAddress": "Ap #781-1741 Sem. St.",
    "zipCode": "130336"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 62,
    "price": 7.26,
    "shipmentAddress": "Ap #766-9767 Etiam Rd.",
    "zipCode": "49-766"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 117,
    "price": 4.59,
    "shipmentAddress": "6213 Tincidunt Road",
    "zipCode": "489962"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 172,
    "price": 5.2,
    "shipmentAddress": "Ap #795-7837 Imperdiet Rd.",
    "zipCode": "123599"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 114,
    "price": 1.13,
    "shipmentAddress": "P.O. Box 623, 6126 Enim Av.",
    "zipCode": "9383"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 132,
    "price": 1.14,
    "shipmentAddress": "Ap #304-433 Eget, St.",
    "zipCode": "32094"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 163,
    "price": 2.76,
    "shipmentAddress": "P.O. Box 132, 2889 Et Rd.",
    "zipCode": "24-490"
  },
  {
    "orderType": "E",
    "orderItemName": "Spice",
    "quantity": 29,
    "price": 9.01,
    "shipmentAddress": "861-4443 Suspendisse Street",
    "zipCode": "00222"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 185,
    "price": 0.04,
    "shipmentAddress": "P.O. Box 991, 442 Dignissim Avenue",
    "zipCode": "40001"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 12,
    "price": 1.34,
    "shipmentAddress": "Ap #825-8923 Molestie Ave",
    "zipCode": "70403"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 191,
    "price": 8.42,
    "shipmentAddress": "P.O. Box 315, 1873 Suscipit Av.",
    "zipCode": "93027"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Toffee",
    "quantity": 197,
    "price": 4,
    "shipmentAddress": "P.O. Box 303, 1597 Nunc Ave",
    "zipCode": "HE1 0VV"
  },
  {
    "orderType": "E",
    "orderItemName": "Cotton Candy",
    "quantity": 111,
    "price": 2.08,
    "shipmentAddress": "3007 Donec Av.",
    "zipCode": "119911"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 100,
    "price": 7.28,
    "shipmentAddress": "564-7769 Et Ave",
    "zipCode": "Y5X 8G9"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 78,
    "price": 6.47,
    "shipmentAddress": "7575 Natoque Rd.",
    "zipCode": "7690"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 11,
    "price": 2.43,
    "shipmentAddress": "8768 Accumsan St.",
    "zipCode": "11700"
  },
  {
    "orderType": "E",
    "orderItemName": "Cake Batter",
    "quantity": 170,
    "price": 1.9,
    "shipmentAddress": "6532 Erat, Road",
    "zipCode": "83567"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 82,
    "price": 6.14,
    "shipmentAddress": "196-8281 Sollicitudin Av.",
    "zipCode": "5979"
  },
  {
    "orderType": "E",
    "orderItemName": "Kiwi",
    "quantity": 18,
    "price": 5.56,
    "shipmentAddress": "1175 Hendrerit Ave",
    "zipCode": "04764"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 142,
    "price": 9.74,
    "shipmentAddress": "8418 Vestibulum, St.",
    "zipCode": "79323"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 115,
    "price": 9.03,
    "shipmentAddress": "Ap #343-959 Lobortis Street",
    "zipCode": "66824"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 122,
    "price": 3.91,
    "shipmentAddress": "989-1309 Neque Rd.",
    "zipCode": "40411"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Ale",
    "quantity": 58,
    "price": 5.24,
    "shipmentAddress": "2057 Posuere St.",
    "zipCode": "983285"
  },
  {
    "orderType": "E",
    "orderItemName": "Kiwi",
    "quantity": 115,
    "price": 2.88,
    "shipmentAddress": "Ap #702-4963 Iaculis Road",
    "zipCode": "8190"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 156,
    "price": 0.28,
    "shipmentAddress": "Ap #668-7319 Tincidunt Road",
    "zipCode": "2611"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 155,
    "price": 8.85,
    "shipmentAddress": "353-4043 Etiam St.",
    "zipCode": "77664"
  },
  {
    "orderType": "E",
    "orderItemName": "Mint",
    "quantity": 85,
    "price": 4.85,
    "shipmentAddress": "986-1871 Consectetuer, Street",
    "zipCode": "35501"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 102,
    "price": 3.93,
    "shipmentAddress": "Ap #345-5596 Non, St.",
    "zipCode": "0544 KI"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 199,
    "price": 5.66,
    "shipmentAddress": "8984 Aliquet Rd.",
    "zipCode": "1636"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 61,
    "price": 1.3,
    "shipmentAddress": "5233 Ac Street",
    "zipCode": "32000"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 190,
    "price": 1.34,
    "shipmentAddress": "778-644 Nullam Ave",
    "zipCode": "20647"
  },
  {
    "orderType": "E",
    "orderItemName": "Punch",
    "quantity": 187,
    "price": 9.8,
    "shipmentAddress": "P.O. Box 335, 1154 Mollis Av.",
    "zipCode": "3955"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 97,
    "price": 6.73,
    "shipmentAddress": "2322 Ac Av.",
    "zipCode": "75866"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 147,
    "price": 1.48,
    "shipmentAddress": "P.O. Box 324, 411 Dis Rd.",
    "zipCode": "9173"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 165,
    "price": 5.93,
    "shipmentAddress": "657-7683 Dis Ave",
    "zipCode": "54-637"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 159,
    "price": 3.44,
    "shipmentAddress": "P.O. Box 309, 9166 Integer Av.",
    "zipCode": "19106"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 84,
    "price": 8.01,
    "shipmentAddress": "4024 Tristique Av.",
    "zipCode": "3693"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango",
    "quantity": 39,
    "price": 0.59,
    "shipmentAddress": "842-4406 Et Street",
    "zipCode": "57414"
  },
  {
    "orderType": "E",
    "orderItemName": "Grand Mariner",
    "quantity": 100,
    "price": 3.57,
    "shipmentAddress": "Ap #822-9897 Molestie. Street",
    "zipCode": "7663"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 63,
    "price": 8.03,
    "shipmentAddress": "933-9716 Lorem Ave",
    "zipCode": "8535"
  },
  {
    "orderType": "E",
    "orderItemName": "Huckleberry",
    "quantity": 137,
    "price": 0.77,
    "shipmentAddress": "P.O. Box 559, 3735 Sodales. Street",
    "zipCode": "440316"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 97,
    "price": 3.95,
    "shipmentAddress": "727-8286 Mi. Avenue",
    "zipCode": "47616"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 179,
    "price": 1.92,
    "shipmentAddress": "P.O. Box 795, 2991 A Av.",
    "zipCode": "5953"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 66,
    "price": 1.75,
    "shipmentAddress": "8204 Tellus Road",
    "zipCode": "8509"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 108,
    "price": 7.63,
    "shipmentAddress": "943-469 Non, Avenue",
    "zipCode": "9994 HV"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 177,
    "price": 4.75,
    "shipmentAddress": "P.O. Box 301, 6454 Ante Street",
    "zipCode": "91276"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 56,
    "price": 1.26,
    "shipmentAddress": "Ap #934-6877 Nibh. Rd.",
    "zipCode": "50-201"
  },
  {
    "orderType": "E",
    "orderItemName": "Dill Pickle",
    "quantity": 75,
    "price": 6.82,
    "shipmentAddress": "962-1034 Eleifend Av.",
    "zipCode": "76942-399"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 81,
    "price": 9.8,
    "shipmentAddress": "9139 Libero Street",
    "zipCode": "3066"
  },
  {
    "orderType": "E",
    "orderItemName": "Pina Colada",
    "quantity": 166,
    "price": 4.3,
    "shipmentAddress": "5061 Cras Rd.",
    "zipCode": "27631"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 149,
    "price": 0.19,
    "shipmentAddress": "Ap #138-1066 Amet Road",
    "zipCode": "278772"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 88,
    "price": 9.84,
    "shipmentAddress": "Ap #690-4883 Fusce St.",
    "zipCode": "88851"
  },
  {
    "orderType": "E",
    "orderItemName": "Sassafras",
    "quantity": 10,
    "price": 4.91,
    "shipmentAddress": "P.O. Box 461, 1528 Magna. Av.",
    "zipCode": "4385 AO"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 111,
    "price": 9.7,
    "shipmentAddress": "Ap #617-1528 Duis Av.",
    "zipCode": "25062"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 13,
    "price": 1.35,
    "shipmentAddress": "8453 Dis St.",
    "zipCode": "158570"
  },
  {
    "orderType": "E",
    "orderItemName": "Graham Cracker",
    "quantity": 95,
    "price": 3.56,
    "shipmentAddress": "4784 Odio, St.",
    "zipCode": "ZF90 2XX"
  },
  {
    "orderType": "E",
    "orderItemName": "Nutmeg",
    "quantity": 105,
    "price": 4.78,
    "shipmentAddress": "9932 Dui, Rd.",
    "zipCode": "838754"
  },
  {
    "orderType": "E",
    "orderItemName": "Spearmint",
    "quantity": 62,
    "price": 2.96,
    "shipmentAddress": "P.O. Box 269, 9228 Dolor. Rd.",
    "zipCode": "215101"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 68,
    "price": 6.26,
    "shipmentAddress": "Ap #194-2892 Ridiculus St.",
    "zipCode": "8446"
  },
  {
    "orderType": "E",
    "orderItemName": "Berry Cola",
    "quantity": 37,
    "price": 9.97,
    "shipmentAddress": "P.O. Box 231, 3827 Nibh. St.",
    "zipCode": "68704-812"
  },
  {
    "orderType": "E",
    "orderItemName": "Bavarian Cream",
    "quantity": 179,
    "price": 9.07,
    "shipmentAddress": "3052 Eleifend St.",
    "zipCode": "21115"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 11,
    "price": 4.14,
    "shipmentAddress": "8497 Conubia Street",
    "zipCode": "49445"
  },
  {
    "orderType": "E",
    "orderItemName": "Bavarian Cream",
    "quantity": 31,
    "price": 1.24,
    "shipmentAddress": "Ap #187-8152 Ornare, St.",
    "zipCode": "RL6 7YQ"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 164,
    "price": 7.79,
    "shipmentAddress": "P.O. Box 724, 731 Pharetra Avenue",
    "zipCode": "55146"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha Irish Cream",
    "quantity": 151,
    "price": 1.72,
    "shipmentAddress": "Ap #393-4268 Eu Street",
    "zipCode": "5519"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 43,
    "price": 1.34,
    "shipmentAddress": "792-6298 At Street",
    "zipCode": "82581"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 16,
    "price": 0.82,
    "shipmentAddress": "9721 Aliquam Road",
    "zipCode": "343634"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 129,
    "price": 8.38,
    "shipmentAddress": "P.O. Box 840, 948 Sapien St.",
    "zipCode": "3398"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 110,
    "price": 5.53,
    "shipmentAddress": "216-1914 Suspendisse Road",
    "zipCode": "51380"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 100,
    "price": 7.27,
    "shipmentAddress": "6023 Luctus Avenue",
    "zipCode": "770668"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Pineapple",
    "quantity": 89,
    "price": 5.95,
    "shipmentAddress": "P.O. Box 545, 6567 Sed Road",
    "zipCode": "07791"
  },
  {
    "orderType": "E",
    "orderItemName": "Amaretto",
    "quantity": 145,
    "price": 1.97,
    "shipmentAddress": "Ap #202-2588 In St.",
    "zipCode": "5290"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 90,
    "price": 0.31,
    "shipmentAddress": "9618 Vulputate Rd.",
    "zipCode": "49034"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 132,
    "price": 0.87,
    "shipmentAddress": "Ap #900-7661 Nisi Road",
    "zipCode": "02692"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 45,
    "price": 1.26,
    "shipmentAddress": "P.O. Box 542, 7421 Dignissim Rd.",
    "zipCode": "07286"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 94,
    "price": 2.24,
    "shipmentAddress": "Ap #100-6159 Ipsum Avenue",
    "zipCode": "15807"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 104,
    "price": 9.72,
    "shipmentAddress": "806 Maecenas Rd.",
    "zipCode": "41513"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 120,
    "price": 4.54,
    "shipmentAddress": "Ap #677-5383 Quisque Road",
    "zipCode": "50275"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 190,
    "price": 8.56,
    "shipmentAddress": "Ap #888-3625 Libero. Rd.",
    "zipCode": "36259"
  },
  {
    "orderType": "E",
    "orderItemName": "Tonic",
    "quantity": 102,
    "price": 4.45,
    "shipmentAddress": "P.O. Box 692, 5436 Est Road",
    "zipCode": "M9H 4K3"
  },
  {
    "orderType": "E",
    "orderItemName": "Cookie Dough",
    "quantity": 88,
    "price": 7.38,
    "shipmentAddress": "8129 Dui Street",
    "zipCode": "507588"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 60,
    "price": 5.26,
    "shipmentAddress": "Ap #500-4128 Pede St.",
    "zipCode": "71261"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Milk",
    "quantity": 60,
    "price": 6.36,
    "shipmentAddress": "751-3652 Orci Avenue",
    "zipCode": "27381"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Milk",
    "quantity": 40,
    "price": 1.2,
    "shipmentAddress": "Ap #194-3239 A, St.",
    "zipCode": "2434"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 193,
    "price": 1.56,
    "shipmentAddress": "9724 Mollis. St.",
    "zipCode": "901218"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 163,
    "price": 0.43,
    "shipmentAddress": "4905 Lobortis. Av.",
    "zipCode": "6600"
  },
  {
    "orderType": "E",
    "orderItemName": "Prickly Pear",
    "quantity": 15,
    "price": 6.8,
    "shipmentAddress": "753-2216 Magnis Ave",
    "zipCode": "668557"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 186,
    "price": 6.32,
    "shipmentAddress": "P.O. Box 942, 2133 Aliquet. Street",
    "zipCode": "PQ3U 6ED"
  },
  {
    "orderType": "E",
    "orderItemName": "Berry Cola",
    "quantity": 160,
    "price": 3.52,
    "shipmentAddress": "556-4465 Eros Street",
    "zipCode": "BT49 1WF"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 128,
    "price": 7.82,
    "shipmentAddress": "456-327 Urna. Street",
    "zipCode": "79917"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 55,
    "price": 3.76,
    "shipmentAddress": "350-295 Ornare Avenue",
    "zipCode": "P9J 7A6"
  },
  {
    "orderType": "E",
    "orderItemName": "Pomegranate",
    "quantity": 173,
    "price": 6.5,
    "shipmentAddress": "Ap #114-7807 Quisque Road",
    "zipCode": "70736"
  },
  {
    "orderType": "E",
    "orderItemName": "Margarita",
    "quantity": 105,
    "price": 1.38,
    "shipmentAddress": "P.O. Box 395, 6357 Donec Street",
    "zipCode": "1072"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 10,
    "price": 6.48,
    "shipmentAddress": "P.O. Box 436, 4058 Eget Ave",
    "zipCode": "70219"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 44,
    "price": 4.2,
    "shipmentAddress": "P.O. Box 631, 6765 Non Street",
    "zipCode": "54607"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 63,
    "price": 7.71,
    "shipmentAddress": "Ap #390-9534 Tempus Rd.",
    "zipCode": "31201"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 152,
    "price": 7.18,
    "shipmentAddress": "709-5169 Erat Avenue",
    "zipCode": "U54 2AU"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 187,
    "price": 2.38,
    "shipmentAddress": "753 Sodales St.",
    "zipCode": "36565"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 141,
    "price": 4.09,
    "shipmentAddress": "2915 Et Street",
    "zipCode": "51924"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 190,
    "price": 7.06,
    "shipmentAddress": "P.O. Box 695, 6226 Ligula. Rd.",
    "zipCode": "72336"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Almond",
    "quantity": 186,
    "price": 7.76,
    "shipmentAddress": "P.O. Box 221, 9514 Augue. Rd.",
    "zipCode": "S4T 5M8"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 167,
    "price": 9.06,
    "shipmentAddress": "P.O. Box 967, 8001 Et Ave",
    "zipCode": "53394"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 169,
    "price": 7.69,
    "shipmentAddress": "8304 Lorem, Av.",
    "zipCode": "43526"
  },
  {
    "orderType": "E",
    "orderItemName": "Margarita",
    "quantity": 34,
    "price": 3.74,
    "shipmentAddress": "623-938 Enim. St.",
    "zipCode": "32742"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 48,
    "price": 6.05,
    "shipmentAddress": "6029 Diam Avenue",
    "zipCode": "41807"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 57,
    "price": 0.03,
    "shipmentAddress": "5630 In Street",
    "zipCode": "128886"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 170,
    "price": 8.13,
    "shipmentAddress": "782-4874 Dolor Av.",
    "zipCode": "28634"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 73,
    "price": 4.54,
    "shipmentAddress": "617-4826 Lorem Av.",
    "zipCode": "3605 CZ"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry Cream Spice",
    "quantity": 106,
    "price": 9.24,
    "shipmentAddress": "Ap #253-4850 Nullam Rd.",
    "zipCode": "01-675"
  },
  {
    "orderType": "E",
    "orderItemName": "Acai Berry",
    "quantity": 112,
    "price": 1.09,
    "shipmentAddress": "P.O. Box 864, 4971 Gravida St.",
    "zipCode": "4818 PY"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 95,
    "price": 7.02,
    "shipmentAddress": "719-1133 Nulla. Street",
    "zipCode": "09862"
  },
  {
    "orderType": "E",
    "orderItemName": "Strawberry",
    "quantity": 44,
    "price": 9.91,
    "shipmentAddress": "P.O. Box 702, 222 Sed Avenue",
    "zipCode": "43-448"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 86,
    "price": 1.56,
    "shipmentAddress": "P.O. Box 986, 9270 Sed Rd.",
    "zipCode": "31-622"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Pineapple",
    "quantity": 157,
    "price": 9.41,
    "shipmentAddress": "950-171 Dui. Avenue",
    "zipCode": "465526"
  },
  {
    "orderType": "E",
    "orderItemName": "Dill Pickle",
    "quantity": 190,
    "price": 7.5,
    "shipmentAddress": "489-4833 Sodales St.",
    "zipCode": "C4Y 5G2"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 56,
    "price": 7.58,
    "shipmentAddress": "3384 Nulla. Street",
    "zipCode": "43662"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 171,
    "price": 9.27,
    "shipmentAddress": "Ap #723-5871 Faucibus St.",
    "zipCode": "80627"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 150,
    "price": 3.17,
    "shipmentAddress": "P.O. Box 659, 2402 Etiam Av.",
    "zipCode": "7672"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 102,
    "price": 0.84,
    "shipmentAddress": "242-3228 Iaculis St.",
    "zipCode": "71419-970"
  },
  {
    "orderType": "E",
    "orderItemName": "Tequila",
    "quantity": 183,
    "price": 5.9,
    "shipmentAddress": "P.O. Box 377, 5872 Nibh St.",
    "zipCode": "61108"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 114,
    "price": 3.5,
    "shipmentAddress": "Ap #577-1637 Ac St.",
    "zipCode": "191294"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 30,
    "price": 3.07,
    "shipmentAddress": "154-2565 Mauris Rd.",
    "zipCode": "80427"
  },
  {
    "orderType": "E",
    "orderItemName": "Strawberry",
    "quantity": 95,
    "price": 1.78,
    "shipmentAddress": "940-3111 Lectus. Rd.",
    "zipCode": "890121"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 115,
    "price": 1.14,
    "shipmentAddress": "Ap #926-2058 Mauris Rd.",
    "zipCode": "57341"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 50,
    "price": 1.98,
    "shipmentAddress": "Ap #228-7538 Enim St.",
    "zipCode": "08014-734"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 101,
    "price": 9.93,
    "shipmentAddress": "P.O. Box 439, 8614 Ad Ave",
    "zipCode": "92801"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 49,
    "price": 6.42,
    "shipmentAddress": "987-3478 Risus Street",
    "zipCode": "95-100"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 191,
    "price": 3.83,
    "shipmentAddress": "Ap #274-7722 Mus. Rd.",
    "zipCode": "193160"
  },
  {
    "orderType": "E",
    "orderItemName": "Malted Milk",
    "quantity": 128,
    "price": 3.68,
    "shipmentAddress": "P.O. Box 923, 1300 Bibendum St.",
    "zipCode": "9383"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 172,
    "price": 7.35,
    "shipmentAddress": "3615 Nunc Ave",
    "zipCode": "1823 CX"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 178,
    "price": 5.27,
    "shipmentAddress": "6373 Non Av.",
    "zipCode": "918568"
  },
  {
    "orderType": "E",
    "orderItemName": "Sarsaparilla",
    "quantity": 58,
    "price": 9.27,
    "shipmentAddress": "292-328 Egestas. St.",
    "zipCode": "OU6 3IF"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 90,
    "price": 6.42,
    "shipmentAddress": "9036 Nibh Street",
    "zipCode": "5018"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 137,
    "price": 3.35,
    "shipmentAddress": "Ap #784-118 Quisque Avenue",
    "zipCode": "07227"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 74,
    "price": 7.78,
    "shipmentAddress": "9912 Duis St.",
    "zipCode": "60633-965"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 103,
    "price": 6.23,
    "shipmentAddress": "Ap #305-4246 Non Av.",
    "zipCode": "452459"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 125,
    "price": 7.95,
    "shipmentAddress": "903-6364 Gravida Avenue",
    "zipCode": "04641"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 35,
    "price": 9.94,
    "shipmentAddress": "P.O. Box 853, 9163 Ornare, Rd.",
    "zipCode": "55989"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Pineapple",
    "quantity": 89,
    "price": 0.87,
    "shipmentAddress": "6364 Massa Av.",
    "zipCode": "G8Z 7P7"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 135,
    "price": 7.46,
    "shipmentAddress": "1590 Arcu Road",
    "zipCode": "5833 QM"
  },
  {
    "orderType": "E",
    "orderItemName": "Birch Beer",
    "quantity": 101,
    "price": 1.24,
    "shipmentAddress": "Ap #948-8103 Erat St.",
    "zipCode": "16-022"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 163,
    "price": 8.96,
    "shipmentAddress": "Ap #771-5977 Ut, Street",
    "zipCode": "0476 NL"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Pineapple",
    "quantity": 178,
    "price": 3.08,
    "shipmentAddress": "P.O. Box 526, 2613 Odio. Avenue",
    "zipCode": "44424"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 84,
    "price": 7.24,
    "shipmentAddress": "Ap #908-2807 Ut, Ave",
    "zipCode": "55088"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 82,
    "price": 8.79,
    "shipmentAddress": "283-5312 Mollis. Ave",
    "zipCode": "11732"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 90,
    "price": 5.36,
    "shipmentAddress": "3354 Ac Ave",
    "zipCode": "50902"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 104,
    "price": 7.51,
    "shipmentAddress": "6592 Ipsum Rd.",
    "zipCode": "7688 NW"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 193,
    "price": 4.7,
    "shipmentAddress": "957-3154 Cursus Ave",
    "zipCode": "72918"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 178,
    "price": 2.41,
    "shipmentAddress": "Ap #680-4940 Ipsum St.",
    "zipCode": "978832"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 65,
    "price": 2.58,
    "shipmentAddress": "P.O. Box 900, 8563 Ut Av.",
    "zipCode": "18951"
  },
  {
    "orderType": "E",
    "orderItemName": "Horchata",
    "quantity": 181,
    "price": 3.59,
    "shipmentAddress": "P.O. Box 834, 1729 Convallis Av.",
    "zipCode": "C0R 1Z9"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 100,
    "price": 8.25,
    "shipmentAddress": "Ap #222-7710 Dignissim. Av.",
    "zipCode": "76498"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Whiskey",
    "quantity": 178,
    "price": 2.49,
    "shipmentAddress": "P.O. Box 571, 4733 Non, Street",
    "zipCode": "26546"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Beer",
    "quantity": 112,
    "price": 2.18,
    "shipmentAddress": "P.O. Box 866, 8473 Risus. Ave",
    "zipCode": "14997"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 185,
    "price": 8.91,
    "shipmentAddress": "722-328 Phasellus Avenue",
    "zipCode": "892582"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 181,
    "price": 5.44,
    "shipmentAddress": "767-1537 Vitae Rd.",
    "zipCode": "55031-707"
  },
  {
    "orderType": "E",
    "orderItemName": "Butterscotch",
    "quantity": 114,
    "price": 5.2,
    "shipmentAddress": "4846 Mauris St.",
    "zipCode": "24080"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 176,
    "price": 8.77,
    "shipmentAddress": "P.O. Box 707, 8880 Nam Av.",
    "zipCode": "09091-822"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 63,
    "price": 0.51,
    "shipmentAddress": "530-1753 Dignissim. Av.",
    "zipCode": "054670"
  },
  {
    "orderType": "E",
    "orderItemName": "Cotton Candy",
    "quantity": 196,
    "price": 3.5,
    "shipmentAddress": "Ap #981-5551 Orci, Avenue",
    "zipCode": "3585"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 192,
    "price": 1.95,
    "shipmentAddress": "Ap #195-5671 Gravida Av.",
    "zipCode": "M46 4EZ"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 194,
    "price": 4.95,
    "shipmentAddress": "Ap #964-4520 Velit St.",
    "zipCode": "209472"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 132,
    "price": 5.6,
    "shipmentAddress": "Ap #296-8203 Curabitur Av.",
    "zipCode": "09-442"
  },
  {
    "orderType": "E",
    "orderItemName": "Nutmeg",
    "quantity": 161,
    "price": 7.35,
    "shipmentAddress": "805-3831 Phasellus Rd.",
    "zipCode": "75284"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 56,
    "price": 5.51,
    "shipmentAddress": "P.O. Box 774, 4346 Luctus Rd.",
    "zipCode": "10429"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 67,
    "price": 6.63,
    "shipmentAddress": "747-533 Magna. Rd.",
    "zipCode": "49-538"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 185,
    "price": 0.63,
    "shipmentAddress": "Ap #681-3568 Non, Road",
    "zipCode": "5210"
  },
  {
    "orderType": "E",
    "orderItemName": "Mint",
    "quantity": 86,
    "price": 7.5,
    "shipmentAddress": "Ap #128-7934 Egestas Rd.",
    "zipCode": "8766"
  },
  {
    "orderType": "E",
    "orderItemName": "Nutmeg",
    "quantity": 156,
    "price": 1.51,
    "shipmentAddress": "P.O. Box 101, 4842 Arcu Rd.",
    "zipCode": "51734"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 154,
    "price": 8.06,
    "shipmentAddress": "255-4918 Dictum Ave",
    "zipCode": "32884"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 104,
    "price": 8.77,
    "shipmentAddress": "Ap #829-4631 Praesent Street",
    "zipCode": "33742"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 161,
    "price": 4.31,
    "shipmentAddress": "1497 Adipiscing St.",
    "zipCode": "86104"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 128,
    "price": 4.92,
    "shipmentAddress": "152-8353 Proin Rd.",
    "zipCode": "91815"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 69,
    "price": 5.86,
    "shipmentAddress": "Ap #925-9259 Proin Street",
    "zipCode": "24-869"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Walnut",
    "quantity": 136,
    "price": 5.79,
    "shipmentAddress": "P.O. Box 891, 4417 Turpis. Rd.",
    "zipCode": "413710"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 165,
    "price": 2.4,
    "shipmentAddress": "P.O. Box 186, 7754 Lorem. Rd.",
    "zipCode": "3950"
  },
  {
    "orderType": "E",
    "orderItemName": "Bacon",
    "quantity": 64,
    "price": 2.85,
    "shipmentAddress": "Ap #824-2501 Ut Rd.",
    "zipCode": "87-658"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 32,
    "price": 5.42,
    "shipmentAddress": "298-2365 Lacinia Street",
    "zipCode": "8306 FD"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 108,
    "price": 6.96,
    "shipmentAddress": "P.O. Box 807, 9229 Aliquet St.",
    "zipCode": "89-117"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 163,
    "price": 7.92,
    "shipmentAddress": "4417 Aliquet Avenue",
    "zipCode": "26339"
  },
  {
    "orderType": "E",
    "orderItemName": "Malted Milk",
    "quantity": 46,
    "price": 3.8,
    "shipmentAddress": "P.O. Box 666, 9952 Dapibus Ave",
    "zipCode": "62573"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 116,
    "price": 2.56,
    "shipmentAddress": "P.O. Box 467, 4064 Ut Ave",
    "zipCode": "30103"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 93,
    "price": 9.72,
    "shipmentAddress": "P.O. Box 713, 1173 Cum St.",
    "zipCode": "552941"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 56,
    "price": 7.27,
    "shipmentAddress": "Ap #267-2331 Enim Av.",
    "zipCode": "3970"
  },
  {
    "orderType": "E",
    "orderItemName": "Birch Beer",
    "quantity": 127,
    "price": 4.05,
    "shipmentAddress": "6189 Lorem Rd.",
    "zipCode": "M4Z 0G6"
  },
  {
    "orderType": "E",
    "orderItemName": "Acai Berry",
    "quantity": 78,
    "price": 3.81,
    "shipmentAddress": "Ap #166-1184 Sociis Avenue",
    "zipCode": "02302"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 13,
    "price": 9.6,
    "shipmentAddress": "Ap #833-4978 Morbi Street",
    "zipCode": "8835"
  },
  {
    "orderType": "E",
    "orderItemName": "Tequila",
    "quantity": 99,
    "price": 5.88,
    "shipmentAddress": "2648 Egestas. St.",
    "zipCode": "863745"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 105,
    "price": 0.17,
    "shipmentAddress": "P.O. Box 480, 738 Pellentesque St.",
    "zipCode": "15112"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 165,
    "price": 3.62,
    "shipmentAddress": "Ap #768-2234 Ornare. Street",
    "zipCode": "61808"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 167,
    "price": 1.62,
    "shipmentAddress": "Ap #801-3086 Blandit St.",
    "zipCode": "5744"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 128,
    "price": 2,
    "shipmentAddress": "P.O. Box 950, 1339 Velit Av.",
    "zipCode": "26171"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 57,
    "price": 7.8,
    "shipmentAddress": "P.O. Box 392, 7437 Amet, Road",
    "zipCode": "15125"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 139,
    "price": 3.9,
    "shipmentAddress": "424-9137 Fermentum Rd.",
    "zipCode": "30399"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 126,
    "price": 5.26,
    "shipmentAddress": "9985 Vulputate, St.",
    "zipCode": "60666-582"
  },
  {
    "orderType": "E",
    "orderItemName": "Pink Lemonade",
    "quantity": 130,
    "price": 5.67,
    "shipmentAddress": "323-2491 Metus Street",
    "zipCode": "2523"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 193,
    "price": 2.21,
    "shipmentAddress": "Ap #413-5736 Arcu. Ave",
    "zipCode": "K3J 0G3"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 120,
    "price": 2.7,
    "shipmentAddress": "157-6360 Accumsan St.",
    "zipCode": "9831"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 100,
    "price": 4.51,
    "shipmentAddress": "9433 Est St.",
    "zipCode": "510211"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 90,
    "price": 6.38,
    "shipmentAddress": "Ap #548-3334 Congue St.",
    "zipCode": "2895"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 61,
    "price": 2.81,
    "shipmentAddress": "P.O. Box 468, 8154 Vestibulum Av.",
    "zipCode": "1911"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 110,
    "price": 8.63,
    "shipmentAddress": "1958 Interdum Street",
    "zipCode": "8234"
  },
  {
    "orderType": "E",
    "orderItemName": "Birch Beer",
    "quantity": 179,
    "price": 7.56,
    "shipmentAddress": "7043 Malesuada St.",
    "zipCode": "716580"
  },
  {
    "orderType": "E",
    "orderItemName": "Pink Lemonade",
    "quantity": 146,
    "price": 4.61,
    "shipmentAddress": "P.O. Box 913, 5490 Fusce Avenue",
    "zipCode": "11007"
  },
  {
    "orderType": "E",
    "orderItemName": "Nutmeg",
    "quantity": 137,
    "price": 2.81,
    "shipmentAddress": "P.O. Box 665, 9433 Non Av.",
    "zipCode": "87684"
  },
  {
    "orderType": "E",
    "orderItemName": "Spice",
    "quantity": 62,
    "price": 8.65,
    "shipmentAddress": "7849 Neque St.",
    "zipCode": "03385"
  },
  {
    "orderType": "E",
    "orderItemName": "Graham Cracker",
    "quantity": 108,
    "price": 3.5,
    "shipmentAddress": "785-9673 Aliquam St.",
    "zipCode": "80917"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 156,
    "price": 1.87,
    "shipmentAddress": "Ap #338-4578 Ut, Avenue",
    "zipCode": "56368"
  },
  {
    "orderType": "E",
    "orderItemName": "Blackberry",
    "quantity": 62,
    "price": 2.21,
    "shipmentAddress": "5990 Suspendisse Ave",
    "zipCode": "983679"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 104,
    "price": 6.2,
    "shipmentAddress": "Ap #808-6673 Non Street",
    "zipCode": "1334"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 84,
    "price": 7.9,
    "shipmentAddress": "Ap #389-750 Id Rd.",
    "zipCode": "8386"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 132,
    "price": 1.26,
    "shipmentAddress": "1676 Dui. Rd.",
    "zipCode": "75183"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 186,
    "price": 7.48,
    "shipmentAddress": "758-8170 Pharetra St.",
    "zipCode": "695950"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 29,
    "price": 5,
    "shipmentAddress": "6358 Neque Rd.",
    "zipCode": "584550"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 165,
    "price": 3.24,
    "shipmentAddress": "100 Faucibus. St.",
    "zipCode": "26072"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 126,
    "price": 8.83,
    "shipmentAddress": "3523 Bibendum St.",
    "zipCode": "91449"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 112,
    "price": 1.73,
    "shipmentAddress": "299-5516 Velit Street",
    "zipCode": "409931"
  },
  {
    "orderType": "E",
    "orderItemName": "Nutmeg",
    "quantity": 110,
    "price": 2.42,
    "shipmentAddress": "Ap #271-3217 Eget Av.",
    "zipCode": "62457"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 93,
    "price": 4.37,
    "shipmentAddress": "491-4463 Ipsum. St.",
    "zipCode": "186733"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 90,
    "price": 8.45,
    "shipmentAddress": "Ap #813-7190 Quis Av.",
    "zipCode": "M7B 1XD"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Beer",
    "quantity": 91,
    "price": 5,
    "shipmentAddress": "P.O. Box 509, 1444 Dictum Ave",
    "zipCode": "337203"
  },
  {
    "orderType": "E",
    "orderItemName": "Margarita",
    "quantity": 58,
    "price": 3.84,
    "shipmentAddress": "216-2590 Massa. Av.",
    "zipCode": "5339"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 156,
    "price": 9.84,
    "shipmentAddress": "P.O. Box 994, 7663 Molestie. Ave",
    "zipCode": "15212-756"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Milk",
    "quantity": 101,
    "price": 9.93,
    "shipmentAddress": "1921 Tristique St.",
    "zipCode": "T8C 2Y5"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 19,
    "price": 6.09,
    "shipmentAddress": "P.O. Box 923, 5807 Auctor. Rd.",
    "zipCode": "31341"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 15,
    "price": 6.28,
    "shipmentAddress": "7743 Amet St.",
    "zipCode": "22732"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 46,
    "price": 3.78,
    "shipmentAddress": "869-1489 Pharetra St.",
    "zipCode": "3891"
  },
  {
    "orderType": "E",
    "orderItemName": "Tonic",
    "quantity": 156,
    "price": 1.41,
    "shipmentAddress": "P.O. Box 423, 6461 Cras St.",
    "zipCode": "4435"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 75,
    "price": 4.41,
    "shipmentAddress": "P.O. Box 288, 2940 Ac Street",
    "zipCode": "30349"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 118,
    "price": 2.77,
    "shipmentAddress": "4037 Varius. Av.",
    "zipCode": "24805"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 137,
    "price": 5.94,
    "shipmentAddress": "P.O. Box 268, 2651 Non Street",
    "zipCode": "U3W 2ZS"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 192,
    "price": 7.24,
    "shipmentAddress": "Ap #886-5923 Pede Ave",
    "zipCode": "9632"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 63,
    "price": 9.99,
    "shipmentAddress": "330-4355 Odio Rd.",
    "zipCode": "97136"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 40,
    "price": 6.99,
    "shipmentAddress": "Ap #434-2417 Senectus Av.",
    "zipCode": "7826"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 68,
    "price": 2.58,
    "shipmentAddress": "Ap #323-6242 Quis Rd.",
    "zipCode": "16812"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 174,
    "price": 1.9,
    "shipmentAddress": "P.O. Box 728, 5767 Mi Road",
    "zipCode": "83897"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 23,
    "price": 6.31,
    "shipmentAddress": "214-6631 Ut, Rd.",
    "zipCode": "907469"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate",
    "quantity": 154,
    "price": 1.47,
    "shipmentAddress": "694-6853 Nam Rd.",
    "zipCode": "82033"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha Irish Cream",
    "quantity": 126,
    "price": 4.9,
    "shipmentAddress": "Ap #928-1141 Nec Ave",
    "zipCode": "67933"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 134,
    "price": 6.77,
    "shipmentAddress": "503-6234 At Av.",
    "zipCode": "04910"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Almond",
    "quantity": 174,
    "price": 0.11,
    "shipmentAddress": "954-2099 Phasellus Street",
    "zipCode": "9547"
  },
  {
    "orderType": "E",
    "orderItemName": "Punch",
    "quantity": 15,
    "price": 3.5,
    "shipmentAddress": "1117 Felis. Rd.",
    "zipCode": "T6S 0Z6"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 163,
    "price": 6.09,
    "shipmentAddress": "885-773 Arcu. Rd.",
    "zipCode": "11515"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Milk",
    "quantity": 122,
    "price": 2.81,
    "shipmentAddress": "P.O. Box 930, 4129 In, Street",
    "zipCode": "C6 1BK"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 62,
    "price": 2.7,
    "shipmentAddress": "Ap #905-406 Luctus Av.",
    "zipCode": "13-281"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Toffee",
    "quantity": 144,
    "price": 6.19,
    "shipmentAddress": "180-819 Felis St.",
    "zipCode": "34710"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 38,
    "price": 7.66,
    "shipmentAddress": "7474 Odio, Rd.",
    "zipCode": "525608"
  },
  {
    "orderType": "E",
    "orderItemName": "Spearmint",
    "quantity": 183,
    "price": 8.09,
    "shipmentAddress": "Ap #500-4298 Lacus. Rd.",
    "zipCode": "99712"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 17,
    "price": 8.07,
    "shipmentAddress": "764-125 Ut, Rd.",
    "zipCode": "39863"
  },
  {
    "orderType": "E",
    "orderItemName": "Bacon",
    "quantity": 68,
    "price": 9.29,
    "shipmentAddress": "518-9200 Non Rd.",
    "zipCode": "490497"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 154,
    "price": 6.97,
    "shipmentAddress": "3181 Amet Avenue",
    "zipCode": "37509-009"
  },
  {
    "orderType": "E",
    "orderItemName": "Sour",
    "quantity": 89,
    "price": 5.85,
    "shipmentAddress": "573-7280 Pretium Road",
    "zipCode": "92637"
  },
  {
    "orderType": "E",
    "orderItemName": "Root Beer",
    "quantity": 30,
    "price": 0.26,
    "shipmentAddress": "6328 Pharetra Ave",
    "zipCode": "8255 WG"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 79,
    "price": 5.05,
    "shipmentAddress": "Ap #400-2663 Ullamcorper. Road",
    "zipCode": "57788"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Whiskey",
    "quantity": 24,
    "price": 6.99,
    "shipmentAddress": "Ap #478-5610 Odio Ave",
    "zipCode": "4622"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 152,
    "price": 9.99,
    "shipmentAddress": "Ap #142-6074 Integer Street",
    "zipCode": "79-933"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 184,
    "price": 6.74,
    "shipmentAddress": "P.O. Box 728, 6079 Et Rd.",
    "zipCode": "10498"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 150,
    "price": 0.8,
    "shipmentAddress": "8447 Quis, Ave",
    "zipCode": "32789"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 84,
    "price": 1.68,
    "shipmentAddress": "Ap #589-9654 Erat Street",
    "zipCode": "56807"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 88,
    "price": 1.27,
    "shipmentAddress": "P.O. Box 772, 8358 Nec Street",
    "zipCode": "54906"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 28,
    "price": 2.38,
    "shipmentAddress": "501-7138 Nec Av.",
    "zipCode": "522101"
  },
  {
    "orderType": "E",
    "orderItemName": "Tropical Punch",
    "quantity": 44,
    "price": 7.5,
    "shipmentAddress": "Ap #284-9045 Malesuada St.",
    "zipCode": "H60 5YO"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 122,
    "price": 9.66,
    "shipmentAddress": "769-1124 Id Street",
    "zipCode": "X0N 2K8"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 60,
    "price": 7.17,
    "shipmentAddress": "Ap #336-2970 Ipsum. Av.",
    "zipCode": "325584"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 44,
    "price": 3.17,
    "shipmentAddress": "P.O. Box 382, 7424 Erat Street",
    "zipCode": "K6K 8E6"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 78,
    "price": 8.96,
    "shipmentAddress": "747-8881 Penatibus St.",
    "zipCode": "7716"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 54,
    "price": 5.84,
    "shipmentAddress": "2948 Convallis St.",
    "zipCode": "826959"
  },
  {
    "orderType": "E",
    "orderItemName": "Mint",
    "quantity": 198,
    "price": 3.7,
    "shipmentAddress": "3070 Aliquam Avenue",
    "zipCode": "14259"
  },
  {
    "orderType": "E",
    "orderItemName": "Bacon",
    "quantity": 83,
    "price": 2.04,
    "shipmentAddress": "Ap #300-4761 Iaculis, Rd.",
    "zipCode": "M1P 5W8"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Cream",
    "quantity": 164,
    "price": 6.8,
    "shipmentAddress": "Ap #342-9471 Suspendisse St.",
    "zipCode": "216594"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 136,
    "price": 1.12,
    "shipmentAddress": "Ap #606-5462 Molestie Rd.",
    "zipCode": "900597"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 50,
    "price": 6.5,
    "shipmentAddress": "382-1427 Penatibus Avenue",
    "zipCode": "A64 9RU"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 70,
    "price": 1.82,
    "shipmentAddress": "P.O. Box 960, 2234 Faucibus Ave",
    "zipCode": "20682"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 17,
    "price": 7.15,
    "shipmentAddress": "391-7634 Mauris. Road",
    "zipCode": "C4J 0B2"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 128,
    "price": 5.33,
    "shipmentAddress": "1285 Mi Street",
    "zipCode": "12-507"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 83,
    "price": 3.7,
    "shipmentAddress": "P.O. Box 956, 5351 Imperdiet St.",
    "zipCode": "041476"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 64,
    "price": 3.19,
    "shipmentAddress": "912-280 Sagittis Rd.",
    "zipCode": "41919"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 165,
    "price": 7.72,
    "shipmentAddress": "274 Luctus. Ave",
    "zipCode": "864267"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 180,
    "price": 9.9,
    "shipmentAddress": "Ap #642-2214 Nulla Ave",
    "zipCode": "50718"
  },
  {
    "orderType": "E",
    "orderItemName": "Root Beer",
    "quantity": 108,
    "price": 0.17,
    "shipmentAddress": "P.O. Box 627, 9306 Consequat Avenue",
    "zipCode": "TX0Z 6FM"
  },
  {
    "orderType": "E",
    "orderItemName": "Peach",
    "quantity": 196,
    "price": 2.93,
    "shipmentAddress": "P.O. Box 303, 1055 Lacus. Ave",
    "zipCode": "59852"
  },
  {
    "orderType": "E",
    "orderItemName": "Amaretto",
    "quantity": 150,
    "price": 9.73,
    "shipmentAddress": "Ap #881-9768 Mi Av.",
    "zipCode": "46821"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 35,
    "price": 3.43,
    "shipmentAddress": "P.O. Box 561, 6076 Magna. Av.",
    "zipCode": "508723"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 184,
    "price": 1.47,
    "shipmentAddress": "8470 Lectus, St.",
    "zipCode": "2333 XV"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 86,
    "price": 7.18,
    "shipmentAddress": "945-3190 Ante, Street",
    "zipCode": "7508"
  },
  {
    "orderType": "E",
    "orderItemName": "Banana",
    "quantity": 124,
    "price": 7.65,
    "shipmentAddress": "487-4311 Sodales Road",
    "zipCode": "1670"
  },
  {
    "orderType": "E",
    "orderItemName": "Butterscotch",
    "quantity": 61,
    "price": 3.85,
    "shipmentAddress": "P.O. Box 865, 1787 Tellus St.",
    "zipCode": "5757"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 118,
    "price": 7.87,
    "shipmentAddress": "P.O. Box 614, 7893 Orci Rd.",
    "zipCode": "4911"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 32,
    "price": 8.57,
    "shipmentAddress": "Ap #586-940 Nunc Road",
    "zipCode": "81104"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 132,
    "price": 5.27,
    "shipmentAddress": "Ap #951-7379 Lobortis. Rd.",
    "zipCode": "02495"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 171,
    "price": 8.26,
    "shipmentAddress": "951-4966 Lorem, Road",
    "zipCode": "9393"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry Ginger Ale",
    "quantity": 162,
    "price": 9.35,
    "shipmentAddress": "5333 Sem Avenue",
    "zipCode": "60709"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 70,
    "price": 6.47,
    "shipmentAddress": "301-5053 Aenean St.",
    "zipCode": "8852"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 186,
    "price": 8.72,
    "shipmentAddress": "Ap #744-3642 Eu Road",
    "zipCode": "89408"
  },
  {
    "orderType": "E",
    "orderItemName": "Peach",
    "quantity": 12,
    "price": 2.66,
    "shipmentAddress": "917-2780 Aliquet Ave",
    "zipCode": "28645"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 20,
    "price": 9.77,
    "shipmentAddress": "P.O. Box 772, 5333 Sagittis Rd.",
    "zipCode": "8849"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 190,
    "price": 0.2,
    "shipmentAddress": "P.O. Box 381, 7913 Ut Rd.",
    "zipCode": "9977"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 194,
    "price": 6.12,
    "shipmentAddress": "Ap #861-9159 Mollis Rd.",
    "zipCode": "20302"
  },
  {
    "orderType": "E",
    "orderItemName": "Prickly Pear",
    "quantity": 61,
    "price": 7.45,
    "shipmentAddress": "P.O. Box 789, 5742 Lacus. St.",
    "zipCode": "8944"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango",
    "quantity": 90,
    "price": 5.26,
    "shipmentAddress": "4361 Mauris. Rd.",
    "zipCode": "H0E 9E8"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Toffee",
    "quantity": 159,
    "price": 4.17,
    "shipmentAddress": "9780 Enim, Rd.",
    "zipCode": "6810"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin",
    "quantity": 160,
    "price": 9.83,
    "shipmentAddress": "P.O. Box 919, 7310 Lorem Rd.",
    "zipCode": "24377-962"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 145,
    "price": 5.11,
    "shipmentAddress": "5238 Integer St.",
    "zipCode": "99423"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 143,
    "price": 0.15,
    "shipmentAddress": "Ap #124-618 Cum Avenue",
    "zipCode": "4057"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 51,
    "price": 9.12,
    "shipmentAddress": "8101 Nec Road",
    "zipCode": "58163"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 124,
    "price": 5.91,
    "shipmentAddress": "792 Nunc St.",
    "zipCode": "85-261"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 42,
    "price": 7.76,
    "shipmentAddress": "619-6887 Sapien, Rd.",
    "zipCode": "484059"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 68,
    "price": 2.86,
    "shipmentAddress": "Ap #519-3666 Eu, Ave",
    "zipCode": "35653"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 104,
    "price": 6.86,
    "shipmentAddress": "Ap #506-3571 Nonummy Avenue",
    "zipCode": "48910"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 166,
    "price": 6.65,
    "shipmentAddress": "Ap #340-1643 Luctus St.",
    "zipCode": "99642"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 70,
    "price": 2.7,
    "shipmentAddress": "Ap #841-9677 Magnis Street",
    "zipCode": "37769"
  },
  {
    "orderType": "E",
    "orderItemName": "Cotton Candy",
    "quantity": 142,
    "price": 7.65,
    "shipmentAddress": "4500 Molestie St.",
    "zipCode": "585091"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry",
    "quantity": 145,
    "price": 9.11,
    "shipmentAddress": "901-4118 Dignissim Rd.",
    "zipCode": "6154 TM"
  },
  {
    "orderType": "E",
    "orderItemName": "Spice",
    "quantity": 108,
    "price": 8.43,
    "shipmentAddress": "P.O. Box 523, 6309 Auctor, Avenue",
    "zipCode": "56991"
  },
  {
    "orderType": "E",
    "orderItemName": "Graham Cracker",
    "quantity": 134,
    "price": 1.68,
    "shipmentAddress": "437-6000 Primis St.",
    "zipCode": "815662"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 112,
    "price": 7.72,
    "shipmentAddress": "136-8671 Vivamus St.",
    "zipCode": "9425"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 145,
    "price": 2.75,
    "shipmentAddress": "3131 Lorem. Ave",
    "zipCode": "7931"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 112,
    "price": 2.74,
    "shipmentAddress": "550-3323 Mollis Ave",
    "zipCode": "55378"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 117,
    "price": 2.4,
    "shipmentAddress": "7104 Vestibulum, Ave",
    "zipCode": "RA7B 0IE"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 96,
    "price": 7.24,
    "shipmentAddress": "Ap #719-3346 Ac Ave",
    "zipCode": "695034"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 58,
    "price": 5.12,
    "shipmentAddress": "614-1454 Ipsum St.",
    "zipCode": "D18 7XW"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry Cream Spice",
    "quantity": 71,
    "price": 5.79,
    "shipmentAddress": "562-9988 Bibendum Rd.",
    "zipCode": "2727"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 165,
    "price": 3.88,
    "shipmentAddress": "Ap #711-3145 Natoque Rd.",
    "zipCode": "4623"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 22,
    "price": 2.08,
    "shipmentAddress": "P.O. Box 769, 8544 Semper, Street",
    "zipCode": "60801"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 93,
    "price": 9.52,
    "shipmentAddress": "P.O. Box 287, 132 At St.",
    "zipCode": "39247-066"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 18,
    "price": 3.85,
    "shipmentAddress": "Ap #772-3407 Lectus St.",
    "zipCode": "630372"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 99,
    "price": 8.97,
    "shipmentAddress": "7095 Dapibus St.",
    "zipCode": "H7W 2KI"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 155,
    "price": 8.8,
    "shipmentAddress": "Ap #843-636 Mattis Ave",
    "zipCode": "85966"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 141,
    "price": 7.79,
    "shipmentAddress": "3495 Vehicula Av.",
    "zipCode": "4742"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 32,
    "price": 3.92,
    "shipmentAddress": "P.O. Box 214, 6554 Augue St.",
    "zipCode": "V2P 4B1"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 191,
    "price": 6.19,
    "shipmentAddress": "Ap #514-5197 Fusce Avenue",
    "zipCode": "R45 1WL"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon",
    "quantity": 10,
    "price": 6.34,
    "shipmentAddress": "P.O. Box 655, 9519 Eu Road",
    "zipCode": "25136"
  },
  {
    "orderType": "E",
    "orderItemName": "Pina Colada",
    "quantity": 115,
    "price": 7.1,
    "shipmentAddress": "P.O. Box 276, 9508 Nunc Road",
    "zipCode": "144508"
  },
  {
    "orderType": "E",
    "orderItemName": "Cookie Dough",
    "quantity": 50,
    "price": 4.93,
    "shipmentAddress": "4891 Quisque Av.",
    "zipCode": "16092"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 138,
    "price": 3.45,
    "shipmentAddress": "Ap #949-5188 Sagittis. Avenue",
    "zipCode": "04393"
  },
  {
    "orderType": "E",
    "orderItemName": "Sour",
    "quantity": 22,
    "price": 2.29,
    "shipmentAddress": "Ap #462-1056 Urna, Avenue",
    "zipCode": "788888"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 107,
    "price": 1.59,
    "shipmentAddress": "Ap #814-1929 Proin Avenue",
    "zipCode": "14728-030"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 144,
    "price": 3.67,
    "shipmentAddress": "5494 Ut St.",
    "zipCode": "51053"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 168,
    "price": 7.39,
    "shipmentAddress": "926-1311 Netus Avenue",
    "zipCode": "2347"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Lime",
    "quantity": 94,
    "price": 5.05,
    "shipmentAddress": "P.O. Box 712, 6182 Odio. Rd.",
    "zipCode": "9232 DJ"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 166,
    "price": 6.6,
    "shipmentAddress": "495-7116 Amet St.",
    "zipCode": "C5G 3H6"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 124,
    "price": 9.5,
    "shipmentAddress": "Ap #339-7549 Odio Street",
    "zipCode": "31970"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Beer",
    "quantity": 131,
    "price": 5.12,
    "shipmentAddress": "6908 Ultricies Street",
    "zipCode": "508403"
  },
  {
    "orderType": "E",
    "orderItemName": "Prickly Pear",
    "quantity": 67,
    "price": 7.03,
    "shipmentAddress": "216-1213 Ultrices Ave",
    "zipCode": "56544"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 151,
    "price": 5.2,
    "shipmentAddress": "8850 Urna Road",
    "zipCode": "60917"
  },
  {
    "orderType": "E",
    "orderItemName": "Cola",
    "quantity": 172,
    "price": 4.83,
    "shipmentAddress": "952-103 Odio St.",
    "zipCode": "28853"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 68,
    "price": 0.1,
    "shipmentAddress": "1118 Ac Ave",
    "zipCode": "60867"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 103,
    "price": 2.8,
    "shipmentAddress": "P.O. Box 627, 1094 Urna Road",
    "zipCode": "8268"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 12,
    "price": 7.95,
    "shipmentAddress": "6174 Enim. Ave",
    "zipCode": "7016"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 11,
    "price": 8.58,
    "shipmentAddress": "848-4573 Et Avenue",
    "zipCode": "7407"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel",
    "quantity": 121,
    "price": 2.55,
    "shipmentAddress": "P.O. Box 464, 4669 Aliquet Street",
    "zipCode": "683654"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 160,
    "price": 4.3,
    "shipmentAddress": "Ap #855-4737 Ac Ave",
    "zipCode": "13643"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha Irish Cream",
    "quantity": 199,
    "price": 3.62,
    "shipmentAddress": "P.O. Box 355, 3252 Ut, Avenue",
    "zipCode": "00312"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 29,
    "price": 7.82,
    "shipmentAddress": "P.O. Box 444, 2907 Non, Street",
    "zipCode": "E6T 9Y0"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 31,
    "price": 3.96,
    "shipmentAddress": "9894 Congue Av.",
    "zipCode": "40789"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 168,
    "price": 0.42,
    "shipmentAddress": "P.O. Box 474, 1204 Lacus. Rd.",
    "zipCode": "02871"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 128,
    "price": 1.78,
    "shipmentAddress": "Ap #422-128 Ullamcorper Rd.",
    "zipCode": "9888"
  },
  {
    "orderType": "E",
    "orderItemName": "Green Onion",
    "quantity": 157,
    "price": 2.89,
    "shipmentAddress": "P.O. Box 551, 7799 Tincidunt St.",
    "zipCode": "B68 5ZN"
  },
  {
    "orderType": "E",
    "orderItemName": "Green Onion",
    "quantity": 137,
    "price": 0.99,
    "shipmentAddress": "Ap #243-9088 Nec Street",
    "zipCode": "FK9 6OI"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 15,
    "price": 2.18,
    "shipmentAddress": "7475 Neque St.",
    "zipCode": "K0M 8K2"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 88,
    "price": 5.7,
    "shipmentAddress": "P.O. Box 328, 5435 Nec, Rd.",
    "zipCode": "9493"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 38,
    "price": 4.04,
    "shipmentAddress": "P.O. Box 595, 4359 Volutpat. Road",
    "zipCode": "52609"
  },
  {
    "orderType": "E",
    "orderItemName": "Tequila",
    "quantity": 71,
    "price": 5.89,
    "shipmentAddress": "365-2385 Aliquet St.",
    "zipCode": "24-051"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 190,
    "price": 3.57,
    "shipmentAddress": "P.O. Box 796, 4248 Curabitur St.",
    "zipCode": "05279"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 68,
    "price": 5.99,
    "shipmentAddress": "521-2338 Tellus Ave",
    "zipCode": "6538"
  },
  {
    "orderType": "E",
    "orderItemName": "Banana",
    "quantity": 29,
    "price": 2.32,
    "shipmentAddress": "P.O. Box 363, 1603 Nunc St.",
    "zipCode": "300872"
  },
  {
    "orderType": "E",
    "orderItemName": "Dill Pickle",
    "quantity": 44,
    "price": 9.6,
    "shipmentAddress": "510-1139 Tincidunt Av.",
    "zipCode": "531035"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 85,
    "price": 2.93,
    "shipmentAddress": "Ap #826-4093 Elit, Road",
    "zipCode": "V7M 5H8"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 131,
    "price": 2.57,
    "shipmentAddress": "461-4596 Morbi St.",
    "zipCode": "9438"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 150,
    "price": 9.21,
    "shipmentAddress": "8789 Nisi Rd.",
    "zipCode": "9579"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 17,
    "price": 9.31,
    "shipmentAddress": "1913 Amet Ave",
    "zipCode": "VY6 4XV"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 200,
    "price": 0.5,
    "shipmentAddress": "P.O. Box 871, 1962 Molestie St.",
    "zipCode": "1652"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan Roll",
    "quantity": 49,
    "price": 3.18,
    "shipmentAddress": "247-5809 Tortor Street",
    "zipCode": "67778"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 13,
    "price": 8.45,
    "shipmentAddress": "9477 Arcu. Rd.",
    "zipCode": "65364"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 126,
    "price": 1.53,
    "shipmentAddress": "P.O. Box 935, 4975 Vulputate Road",
    "zipCode": "756434"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 97,
    "price": 7.57,
    "shipmentAddress": "P.O. Box 520, 270 Amet St.",
    "zipCode": "874839"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Walnut",
    "quantity": 29,
    "price": 1.4,
    "shipmentAddress": "4875 Erat. Av.",
    "zipCode": "91116"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 74,
    "price": 2.25,
    "shipmentAddress": "254 Sem Av.",
    "zipCode": "88056"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 17,
    "price": 9.01,
    "shipmentAddress": "P.O. Box 305, 5203 Elit, Rd.",
    "zipCode": "71228"
  },
  {
    "orderType": "E",
    "orderItemName": "Pistachio",
    "quantity": 98,
    "price": 0.26,
    "shipmentAddress": "791-4036 Nullam Avenue",
    "zipCode": "31902"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 93,
    "price": 1.11,
    "shipmentAddress": "874-8648 Vel, Avenue",
    "zipCode": "93856"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 66,
    "price": 4.6,
    "shipmentAddress": "P.O. Box 817, 2998 Erat. Road",
    "zipCode": "58707"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 89,
    "price": 7.77,
    "shipmentAddress": "P.O. Box 870, 9422 Adipiscing Rd.",
    "zipCode": "97572"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 40,
    "price": 4.6,
    "shipmentAddress": "P.O. Box 487, 2204 Tellus. Avenue",
    "zipCode": "7262"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 88,
    "price": 9.27,
    "shipmentAddress": "P.O. Box 286, 6564 Purus. Road",
    "zipCode": "10208"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 26,
    "price": 3.39,
    "shipmentAddress": "P.O. Box 647, 8329 Diam Avenue",
    "zipCode": "2282"
  },
  {
    "orderType": "E",
    "orderItemName": "Tropical Punch",
    "quantity": 87,
    "price": 8.16,
    "shipmentAddress": "Ap #288-5623 Sollicitudin Street",
    "zipCode": "23853"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 47,
    "price": 6.07,
    "shipmentAddress": "Ap #635-7521 Nunc Av.",
    "zipCode": "55691"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 127,
    "price": 7.77,
    "shipmentAddress": "Ap #810-4518 Orci, Avenue",
    "zipCode": "7409"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 84,
    "price": 2.04,
    "shipmentAddress": "6551 Pede Street",
    "zipCode": "96872"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 91,
    "price": 4.99,
    "shipmentAddress": "Ap #847-949 Consectetuer Rd.",
    "zipCode": "895511"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 15,
    "price": 5.42,
    "shipmentAddress": "315-6557 A Avenue",
    "zipCode": "99246"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 156,
    "price": 7.69,
    "shipmentAddress": "3811 Eros. Rd.",
    "zipCode": "U0 9RQ"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 16,
    "price": 3.94,
    "shipmentAddress": "235-7497 Pellentesque, St.",
    "zipCode": "57516"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 44,
    "price": 1.86,
    "shipmentAddress": "P.O. Box 909, 2837 Turpis Av.",
    "zipCode": "41109"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 14,
    "price": 9.63,
    "shipmentAddress": "289-2176 Interdum St.",
    "zipCode": "8205"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 54,
    "price": 4.04,
    "shipmentAddress": "332-944 Semper St.",
    "zipCode": "07888"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 200,
    "price": 6.22,
    "shipmentAddress": "Ap #616-4804 Mauris Avenue",
    "zipCode": "74283-886"
  },
  {
    "orderType": "E",
    "orderItemName": "Malted Milk",
    "quantity": 175,
    "price": 5.9,
    "shipmentAddress": "2616 Purus. Street",
    "zipCode": "371659"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 20,
    "price": 9.49,
    "shipmentAddress": "Ap #950-5648 Mi Av.",
    "zipCode": "27342"
  },
  {
    "orderType": "E",
    "orderItemName": "Cake Batter",
    "quantity": 68,
    "price": 2.47,
    "shipmentAddress": "8080 Hendrerit. Street",
    "zipCode": "7603"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry Ginger Ale",
    "quantity": 79,
    "price": 5.65,
    "shipmentAddress": "3565 Molestie Rd.",
    "zipCode": "95368"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 188,
    "price": 4.55,
    "shipmentAddress": "Ap #331-2444 Eget Rd.",
    "zipCode": "35331-730"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 42,
    "price": 3.53,
    "shipmentAddress": "808-8375 Massa St.",
    "zipCode": "97-503"
  },
  {
    "orderType": "E",
    "orderItemName": "Carmel Apple",
    "quantity": 53,
    "price": 8.59,
    "shipmentAddress": "386-4271 Ac Av.",
    "zipCode": "6614"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 10,
    "price": 7.69,
    "shipmentAddress": "P.O. Box 585, 8865 Parturient Av.",
    "zipCode": "63447"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin",
    "quantity": 140,
    "price": 7.03,
    "shipmentAddress": "648 Eu Rd.",
    "zipCode": "044480"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 23,
    "price": 0.24,
    "shipmentAddress": "8269 Nec, St.",
    "zipCode": "33732"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 139,
    "price": 6.3,
    "shipmentAddress": "Ap #898-9810 Leo, St.",
    "zipCode": "X71 8AV"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 64,
    "price": 8.13,
    "shipmentAddress": "Ap #500-7058 Cras Ave",
    "zipCode": "78516-729"
  },
  {
    "orderType": "E",
    "orderItemName": "Cranberry",
    "quantity": 192,
    "price": 1.89,
    "shipmentAddress": "Ap #651-4778 Tincidunt Ave",
    "zipCode": "51061"
  },
  {
    "orderType": "E",
    "orderItemName": "Acai Berry",
    "quantity": 185,
    "price": 6.97,
    "shipmentAddress": "8780 Pede, Road",
    "zipCode": "2601"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 22,
    "price": 1.85,
    "shipmentAddress": "4679 Arcu Street",
    "zipCode": "04022"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 97,
    "price": 4.34,
    "shipmentAddress": "Ap #531-151 Metus Avenue",
    "zipCode": "64-207"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 163,
    "price": 2.34,
    "shipmentAddress": "Ap #526-2527 Ipsum Rd.",
    "zipCode": "407296"
  },
  {
    "orderType": "E",
    "orderItemName": "Horchata",
    "quantity": 50,
    "price": 9.44,
    "shipmentAddress": "Ap #141-2070 Cursus St.",
    "zipCode": "15705"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 94,
    "price": 2.31,
    "shipmentAddress": "P.O. Box 742, 7769 Placerat, Road",
    "zipCode": "86377"
  },
  {
    "orderType": "E",
    "orderItemName": "Doughnut",
    "quantity": 68,
    "price": 8.4,
    "shipmentAddress": "161-7158 Auctor Ave",
    "zipCode": "31-428"
  },
  {
    "orderType": "E",
    "orderItemName": "Huckleberry",
    "quantity": 128,
    "price": 6.4,
    "shipmentAddress": "332-8209 Mollis. St.",
    "zipCode": "PK7 5JP"
  },
  {
    "orderType": "E",
    "orderItemName": "Papaya",
    "quantity": 49,
    "price": 3.78,
    "shipmentAddress": "P.O. Box 724, 1076 In Road",
    "zipCode": "40028"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 68,
    "price": 6.33,
    "shipmentAddress": "Ap #765-7063 Nunc Street",
    "zipCode": "5234 LO"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 141,
    "price": 4.95,
    "shipmentAddress": "232-1222 Placerat Road",
    "zipCode": "9764"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 127,
    "price": 4.37,
    "shipmentAddress": "P.O. Box 687, 6935 Mauris Road",
    "zipCode": "MO13 2CE"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Beer",
    "quantity": 17,
    "price": 3.98,
    "shipmentAddress": "P.O. Box 733, 7959 Nec Avenue",
    "zipCode": "171710"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 181,
    "price": 9.76,
    "shipmentAddress": "4559 Vitae Rd.",
    "zipCode": "25213"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 165,
    "price": 6.92,
    "shipmentAddress": "Ap #654-1752 Ante Avenue",
    "zipCode": "144433"
  },
  {
    "orderType": "E",
    "orderItemName": "Pistachio",
    "quantity": 182,
    "price": 0.02,
    "shipmentAddress": "411-2403 Id Street",
    "zipCode": "2031"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 164,
    "price": 2.73,
    "shipmentAddress": "Ap #948-678 Mauris St.",
    "zipCode": "994866"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 18,
    "price": 3.74,
    "shipmentAddress": "324-2458 Purus. St.",
    "zipCode": "C8 3RK"
  },
  {
    "orderType": "E",
    "orderItemName": "Cola",
    "quantity": 115,
    "price": 0.03,
    "shipmentAddress": "963-7591 Diam. Avenue",
    "zipCode": "G6S 2N0"
  },
  {
    "orderType": "E",
    "orderItemName": "Tonic",
    "quantity": 92,
    "price": 1.27,
    "shipmentAddress": "P.O. Box 154, 9137 Rutrum St.",
    "zipCode": "60254"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 35,
    "price": 3.4,
    "shipmentAddress": "457-9782 Ac Street",
    "zipCode": "70200"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 89,
    "price": 8.3,
    "shipmentAddress": "789-3293 Pretium Road",
    "zipCode": "92678"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 103,
    "price": 1.6,
    "shipmentAddress": "P.O. Box 618, 9042 Blandit St.",
    "zipCode": "23204"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry",
    "quantity": 59,
    "price": 7.88,
    "shipmentAddress": "5689 Sit Road",
    "zipCode": "8389"
  },
  {
    "orderType": "E",
    "orderItemName": "Tonic",
    "quantity": 48,
    "price": 2.48,
    "shipmentAddress": "P.O. Box 252, 9899 Interdum Av.",
    "zipCode": "C1N 5S6"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 124,
    "price": 4.76,
    "shipmentAddress": "Ap #563-2209 Malesuada St.",
    "zipCode": "86003"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 88,
    "price": 1.95,
    "shipmentAddress": "Ap #865-7425 Fringilla Road",
    "zipCode": "942579"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 49,
    "price": 5.49,
    "shipmentAddress": "7540 Mi. St.",
    "zipCode": "6612"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 107,
    "price": 6.16,
    "shipmentAddress": "Ap #400-732 Lacus, Rd.",
    "zipCode": "1576"
  },
  {
    "orderType": "E",
    "orderItemName": "Birch Beer",
    "quantity": 22,
    "price": 5.22,
    "shipmentAddress": "Ap #941-8318 Fusce St.",
    "zipCode": "6756"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 134,
    "price": 1.89,
    "shipmentAddress": "P.O. Box 547, 9773 Risus. Rd.",
    "zipCode": "62480"
  },
  {
    "orderType": "E",
    "orderItemName": "Pomegranate",
    "quantity": 90,
    "price": 8.7,
    "shipmentAddress": "9087 At, Rd.",
    "zipCode": "83260"
  },
  {
    "orderType": "E",
    "orderItemName": "Tequila",
    "quantity": 20,
    "price": 0.24,
    "shipmentAddress": "415-2575 Integer Road",
    "zipCode": "42913"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 161,
    "price": 8.51,
    "shipmentAddress": "843-4526 Risus. St.",
    "zipCode": "88834"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 44,
    "price": 1.14,
    "shipmentAddress": "5168 Ullamcorper, Av.",
    "zipCode": "47427"
  },
  {
    "orderType": "E",
    "orderItemName": "Bavarian Cream",
    "quantity": 184,
    "price": 3.22,
    "shipmentAddress": "1009 Massa. Ave",
    "zipCode": "30011-393"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry Cream Spice",
    "quantity": 117,
    "price": 0.04,
    "shipmentAddress": "P.O. Box 903, 3926 Enim St.",
    "zipCode": "16480"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 91,
    "price": 9.16,
    "shipmentAddress": "125 Dolor. Avenue",
    "zipCode": "9093 ZN"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Lime",
    "quantity": 171,
    "price": 1.3,
    "shipmentAddress": "Ap #299-9813 Lectus St.",
    "zipCode": "961776"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 196,
    "price": 2.86,
    "shipmentAddress": "687-3582 Pharetra Rd.",
    "zipCode": "238512"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 70,
    "price": 6.09,
    "shipmentAddress": "P.O. Box 566, 7431 Laoreet, St.",
    "zipCode": "75365"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 198,
    "price": 9.94,
    "shipmentAddress": "4853 Amet Avenue",
    "zipCode": "689721"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 95,
    "price": 6.09,
    "shipmentAddress": "P.O. Box 397, 8218 Molestie Street",
    "zipCode": "4459"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 137,
    "price": 3.3,
    "shipmentAddress": "P.O. Box 852, 937 Commodo Rd.",
    "zipCode": "50716"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 46,
    "price": 4.26,
    "shipmentAddress": "7462 Ultrices. St.",
    "zipCode": "61610"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 60,
    "price": 4.1,
    "shipmentAddress": "818-4291 In St.",
    "zipCode": "8618"
  },
  {
    "orderType": "E",
    "orderItemName": "Acai Berry",
    "quantity": 158,
    "price": 3.48,
    "shipmentAddress": "234-8149 Arcu St.",
    "zipCode": "R5A 6B7"
  },
  {
    "orderType": "E",
    "orderItemName": "Cranberry",
    "quantity": 89,
    "price": 8.83,
    "shipmentAddress": "6056 Ornare, Avenue",
    "zipCode": "F05 6OQ"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry Ginger Ale",
    "quantity": 100,
    "price": 0.22,
    "shipmentAddress": "Ap #201-4901 In St.",
    "zipCode": "7357"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 108,
    "price": 7.55,
    "shipmentAddress": "781-8490 Magna St.",
    "zipCode": "2485 GW"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 83,
    "price": 6.7,
    "shipmentAddress": "Ap #666-7479 Mi. Avenue",
    "zipCode": "5362"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango",
    "quantity": 137,
    "price": 9.9,
    "shipmentAddress": "P.O. Box 966, 3911 Neque Road",
    "zipCode": "5767"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon",
    "quantity": 142,
    "price": 2.97,
    "shipmentAddress": "8444 Vel, St.",
    "zipCode": "74272"
  },
  {
    "orderType": "E",
    "orderItemName": "Cranberry",
    "quantity": 117,
    "price": 3.75,
    "shipmentAddress": "Ap #100-1020 Sollicitudin Ave",
    "zipCode": "390568"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 56,
    "price": 3.95,
    "shipmentAddress": "168-2826 Ipsum Road",
    "zipCode": "668607"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 79,
    "price": 9.79,
    "shipmentAddress": "Ap #751-3914 Cum Rd.",
    "zipCode": "69459"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon",
    "quantity": 31,
    "price": 5.84,
    "shipmentAddress": "Ap #968-5000 Pellentesque Road",
    "zipCode": "L4F 8GM"
  },
  {
    "orderType": "E",
    "orderItemName": "Peach",
    "quantity": 39,
    "price": 6.72,
    "shipmentAddress": "6613 Ornare. Avenue",
    "zipCode": "2403"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 82,
    "price": 5.17,
    "shipmentAddress": "343-3391 Enim Rd.",
    "zipCode": "62164"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 129,
    "price": 6.78,
    "shipmentAddress": "Ap #408-9586 Blandit. Avenue",
    "zipCode": "21946-597"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 18,
    "price": 0.01,
    "shipmentAddress": "7673 Risus. Street",
    "zipCode": "5854 HL"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 169,
    "price": 8.5,
    "shipmentAddress": "P.O. Box 709, 8954 Aliquam Av.",
    "zipCode": "ZK8O 5LH"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Beer",
    "quantity": 160,
    "price": 1.35,
    "shipmentAddress": "Ap #942-7828 Ullamcorper Rd.",
    "zipCode": "9180 IH"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 149,
    "price": 9.54,
    "shipmentAddress": "Ap #200-380 Semper Av.",
    "zipCode": "R9G 7X9"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 17,
    "price": 9.89,
    "shipmentAddress": "P.O. Box 876, 7346 Tincidunt, Road",
    "zipCode": "61908"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 102,
    "price": 1.83,
    "shipmentAddress": "P.O. Box 136, 496 Fringilla, Av.",
    "zipCode": "4947"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 112,
    "price": 3.66,
    "shipmentAddress": "P.O. Box 621, 307 Ipsum Road",
    "zipCode": "50005"
  },
  {
    "orderType": "E",
    "orderItemName": "Bacon",
    "quantity": 126,
    "price": 5.56,
    "shipmentAddress": "Ap #177-5009 In Rd.",
    "zipCode": "61482"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 75,
    "price": 7.8,
    "shipmentAddress": "P.O. Box 541, 4002 Sit St.",
    "zipCode": "85048"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 49,
    "price": 9.4,
    "shipmentAddress": "Ap #244-5523 Mauris, Av.",
    "zipCode": "BB5D 9XZ"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 134,
    "price": 6.71,
    "shipmentAddress": "Ap #746-5011 Cum St.",
    "zipCode": "10700"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 53,
    "price": 0.37,
    "shipmentAddress": "P.O. Box 486, 9278 Aliquam Avenue",
    "zipCode": "1396 WM"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 166,
    "price": 8.69,
    "shipmentAddress": "Ap #902-4366 Accumsan Ave",
    "zipCode": "49243"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 126,
    "price": 5.55,
    "shipmentAddress": "P.O. Box 683, 2204 Sed Avenue",
    "zipCode": "YK5J 3ZB"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 169,
    "price": 3.92,
    "shipmentAddress": "Ap #196-7553 Suspendisse Av.",
    "zipCode": "46400-067"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 12,
    "price": 0.31,
    "shipmentAddress": "577-1087 Ultrices. Avenue",
    "zipCode": "CO9Y 9EC"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 85,
    "price": 8.75,
    "shipmentAddress": "P.O. Box 613, 254 Phasellus Avenue",
    "zipCode": "118935"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 65,
    "price": 4.87,
    "shipmentAddress": "P.O. Box 865, 3874 Pulvinar Rd.",
    "zipCode": "FE2 6WZ"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 32,
    "price": 6.45,
    "shipmentAddress": "315-7035 Pede. Rd.",
    "zipCode": "6065"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 19,
    "price": 4.91,
    "shipmentAddress": "9309 Nunc. Rd.",
    "zipCode": "C6G 2K8"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 88,
    "price": 0.92,
    "shipmentAddress": "P.O. Box 312, 1816 Vestibulum Rd.",
    "zipCode": "65304"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 184,
    "price": 7.26,
    "shipmentAddress": "P.O. Box 936, 1881 Dolor St.",
    "zipCode": "46477"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Lime",
    "quantity": 53,
    "price": 7.76,
    "shipmentAddress": "P.O. Box 925, 802 Dui Av.",
    "zipCode": "11733-630"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 182,
    "price": 5.39,
    "shipmentAddress": "Ap #479-6449 Nisi. Road",
    "zipCode": "61706"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 153,
    "price": 2.28,
    "shipmentAddress": "Ap #587-3717 Et St.",
    "zipCode": "48150"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 75,
    "price": 2.7,
    "shipmentAddress": "2659 Et Av.",
    "zipCode": "5256"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 63,
    "price": 4.3,
    "shipmentAddress": "P.O. Box 923, 8978 Ornare Street",
    "zipCode": "95399"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 103,
    "price": 2.92,
    "shipmentAddress": "102-1455 Magna. St.",
    "zipCode": "71714"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 61,
    "price": 4.43,
    "shipmentAddress": "389-5461 Erat, St.",
    "zipCode": "4457"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Milk",
    "quantity": 59,
    "price": 0.54,
    "shipmentAddress": "1035 Tellus. St.",
    "zipCode": "726689"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 127,
    "price": 3.29,
    "shipmentAddress": "P.O. Box 734, 2598 Ac St.",
    "zipCode": "103966"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 120,
    "price": 9.75,
    "shipmentAddress": "Ap #575-7292 Proin Rd.",
    "zipCode": "9103"
  },
  {
    "orderType": "E",
    "orderItemName": "Prickly Pear",
    "quantity": 11,
    "price": 8.81,
    "shipmentAddress": "8719 Nec Rd.",
    "zipCode": "7876"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 136,
    "price": 8.59,
    "shipmentAddress": "P.O. Box 432, 2695 Turpis Street",
    "zipCode": "0306 CI"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 37,
    "price": 7.3,
    "shipmentAddress": "Ap #422-7075 Orci, Avenue",
    "zipCode": "99896"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Ale",
    "quantity": 54,
    "price": 9.61,
    "shipmentAddress": "P.O. Box 168, 8988 Mauris St.",
    "zipCode": "W7 6YQ"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 156,
    "price": 5.44,
    "shipmentAddress": "8718 Lacus. Rd.",
    "zipCode": "722845"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 143,
    "price": 4.35,
    "shipmentAddress": "805-4057 Sed Avenue",
    "zipCode": "17104"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate",
    "quantity": 10,
    "price": 2.68,
    "shipmentAddress": "Ap #846-2237 Elit. Rd.",
    "zipCode": "27049-852"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 100,
    "price": 3.27,
    "shipmentAddress": "P.O. Box 309, 3351 Vel, Av.",
    "zipCode": "23088"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 36,
    "price": 1.62,
    "shipmentAddress": "Ap #776-9951 Tincidunt St.",
    "zipCode": "27509"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 76,
    "price": 7.33,
    "shipmentAddress": "P.O. Box 546, 6812 Sem Ave",
    "zipCode": "178368"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 11,
    "price": 9.62,
    "shipmentAddress": "Ap #286-7181 Venenatis St.",
    "zipCode": "109588"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 190,
    "price": 2.24,
    "shipmentAddress": "1952 Neque. Av.",
    "zipCode": "18-660"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 19,
    "price": 6.68,
    "shipmentAddress": "Ap #403-9651 Phasellus Rd.",
    "zipCode": "943982"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 191,
    "price": 3.44,
    "shipmentAddress": "Ap #690-8350 Malesuada Rd.",
    "zipCode": "355044"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 36,
    "price": 9.98,
    "shipmentAddress": "9810 Ante, Rd.",
    "zipCode": "01025"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 17,
    "price": 4.08,
    "shipmentAddress": "Ap #147-2407 Sem Rd.",
    "zipCode": "78517-542"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 99,
    "price": 6.48,
    "shipmentAddress": "P.O. Box 371, 505 Praesent Street",
    "zipCode": "88713"
  },
  {
    "orderType": "E",
    "orderItemName": "Strawberry",
    "quantity": 121,
    "price": 5.66,
    "shipmentAddress": "3802 Lacus Rd.",
    "zipCode": "65814"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 138,
    "price": 9.03,
    "shipmentAddress": "P.O. Box 305, 6823 Pharetra St.",
    "zipCode": "7179"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 143,
    "price": 3.68,
    "shipmentAddress": "329-2476 Ut Ave",
    "zipCode": "25113"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 23,
    "price": 1.1,
    "shipmentAddress": "4130 Auctor Rd.",
    "zipCode": "89-977"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 168,
    "price": 8.19,
    "shipmentAddress": "979-3052 Sem. Rd.",
    "zipCode": "61641"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 13,
    "price": 0,
    "shipmentAddress": "Ap #804-8487 Consectetuer Ave",
    "zipCode": "40659"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 168,
    "price": 8.4,
    "shipmentAddress": "Ap #613-8133 Natoque Ave",
    "zipCode": "74542"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 196,
    "price": 8.57,
    "shipmentAddress": "P.O. Box 716, 3262 Morbi Avenue",
    "zipCode": "8298"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 148,
    "price": 4.98,
    "shipmentAddress": "271-5340 Nulla. Street",
    "zipCode": "H3M 3R8"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 74,
    "price": 7.34,
    "shipmentAddress": "9040 Eu Avenue",
    "zipCode": "435369"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 42,
    "price": 5.38,
    "shipmentAddress": "6663 Et Rd.",
    "zipCode": "A1H 8G8"
  },
  {
    "orderType": "E",
    "orderItemName": "Margarita",
    "quantity": 36,
    "price": 8.36,
    "shipmentAddress": "Ap #522-7952 Nunc Avenue",
    "zipCode": "5070"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla Cream",
    "quantity": 49,
    "price": 6.16,
    "shipmentAddress": "1274 Tempus Rd.",
    "zipCode": "B4K 6N7"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 185,
    "price": 4.83,
    "shipmentAddress": "5266 Neque Avenue",
    "zipCode": "51221-687"
  },
  {
    "orderType": "E",
    "orderItemName": "Strawberry Kiwi",
    "quantity": 149,
    "price": 5.49,
    "shipmentAddress": "513 Sed Road",
    "zipCode": "36-170"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 153,
    "price": 9.28,
    "shipmentAddress": "Ap #406-4499 Arcu. St.",
    "zipCode": "516462"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 41,
    "price": 7,
    "shipmentAddress": "P.O. Box 380, 5959 Ridiculus St.",
    "zipCode": "9165"
  },
  {
    "orderType": "E",
    "orderItemName": "Pina Colada",
    "quantity": 121,
    "price": 4.34,
    "shipmentAddress": "4274 Eu Rd.",
    "zipCode": "J8M 3N7"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 80,
    "price": 1.91,
    "shipmentAddress": "Ap #615-4436 Praesent St.",
    "zipCode": "V5G 9L1"
  },
  {
    "orderType": "E",
    "orderItemName": "Berry Cola",
    "quantity": 177,
    "price": 5.35,
    "shipmentAddress": "5617 Adipiscing Rd.",
    "zipCode": "UM8 0BQ"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 130,
    "price": 2.63,
    "shipmentAddress": "P.O. Box 663, 5977 Pede Av.",
    "zipCode": "9735"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 31,
    "price": 0.31,
    "shipmentAddress": "3893 Fringilla Av.",
    "zipCode": "24479"
  },
  {
    "orderType": "E",
    "orderItemName": "Horchata",
    "quantity": 100,
    "price": 5.53,
    "shipmentAddress": "622-7390 Est, Ave",
    "zipCode": "8759"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 153,
    "price": 2.29,
    "shipmentAddress": "5784 Blandit Ave",
    "zipCode": "38294"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 78,
    "price": 4.41,
    "shipmentAddress": "5589 In Avenue",
    "zipCode": "74440"
  },
  {
    "orderType": "E",
    "orderItemName": "Nutmeg",
    "quantity": 128,
    "price": 3.29,
    "shipmentAddress": "Ap #327-4220 Phasellus Ave",
    "zipCode": "1032 US"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 134,
    "price": 3.36,
    "shipmentAddress": "8918 Elit, St.",
    "zipCode": "44211"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Cream",
    "quantity": 30,
    "price": 7.42,
    "shipmentAddress": "811-4751 Porttitor St.",
    "zipCode": "90012"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 93,
    "price": 7.92,
    "shipmentAddress": "659-6483 Lobortis St.",
    "zipCode": "K62 3BU"
  },
  {
    "orderType": "E",
    "orderItemName": "Banana",
    "quantity": 198,
    "price": 9.88,
    "shipmentAddress": "Ap #528-5598 Id Rd.",
    "zipCode": "K5R 3H8"
  },
  {
    "orderType": "E",
    "orderItemName": "Butterscotch",
    "quantity": 39,
    "price": 2.29,
    "shipmentAddress": "Ap #654-1903 Purus Street",
    "zipCode": "10602"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 58,
    "price": 4.53,
    "shipmentAddress": "1128 Ultricies Rd.",
    "zipCode": "7370"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 184,
    "price": 5.87,
    "shipmentAddress": "5855 Curabitur St.",
    "zipCode": "43258"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 36,
    "price": 9.53,
    "shipmentAddress": "784-4494 Convallis, Avenue",
    "zipCode": "0148 IQ"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 65,
    "price": 8.46,
    "shipmentAddress": "P.O. Box 472, 9543 Eu Rd.",
    "zipCode": "5250"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 137,
    "price": 6.58,
    "shipmentAddress": "P.O. Box 670, 4020 Primis St.",
    "zipCode": "5830"
  },
  {
    "orderType": "E",
    "orderItemName": "Sarsaparilla",
    "quantity": 133,
    "price": 5.19,
    "shipmentAddress": "P.O. Box 808, 1157 Quam. St.",
    "zipCode": "41211"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 81,
    "price": 2.36,
    "shipmentAddress": "4114 Imperdiet Av.",
    "zipCode": "69833"
  },
  {
    "orderType": "E",
    "orderItemName": "Acai Berry",
    "quantity": 108,
    "price": 9.09,
    "shipmentAddress": "469-6210 Eu Rd.",
    "zipCode": "2222"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 104,
    "price": 2.85,
    "shipmentAddress": "7184 Lobortis Ave",
    "zipCode": "03352"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 118,
    "price": 1.99,
    "shipmentAddress": "2455 Eu Street",
    "zipCode": "850944"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 162,
    "price": 0.33,
    "shipmentAddress": "681-8601 Nunc Avenue",
    "zipCode": "3406"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemonade",
    "quantity": 122,
    "price": 9.92,
    "shipmentAddress": "Ap #301-1910 Augue Street",
    "zipCode": "33940"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 172,
    "price": 5.2,
    "shipmentAddress": "P.O. Box 467, 3654 Molestie Ave",
    "zipCode": "43159"
  },
  {
    "orderType": "E",
    "orderItemName": "Cookie Dough",
    "quantity": 106,
    "price": 6.64,
    "shipmentAddress": "Ap #204-7907 Non Rd.",
    "zipCode": "651799"
  },
  {
    "orderType": "E",
    "orderItemName": "Punch",
    "quantity": 16,
    "price": 2.15,
    "shipmentAddress": "P.O. Box 881, 1587 Mollis. Rd.",
    "zipCode": "842347"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Milk",
    "quantity": 95,
    "price": 5.2,
    "shipmentAddress": "557-2393 Nec, Av.",
    "zipCode": "63658"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 165,
    "price": 6.97,
    "shipmentAddress": "144-5209 Libero. Ave",
    "zipCode": "3152"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 101,
    "price": 3.45,
    "shipmentAddress": "P.O. Box 381, 5146 Id, Rd.",
    "zipCode": "17264"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 79,
    "price": 4.33,
    "shipmentAddress": "Ap #964-7232 Nisi St.",
    "zipCode": "97515"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 64,
    "price": 5.68,
    "shipmentAddress": "P.O. Box 169, 2135 Porttitor Rd.",
    "zipCode": "50-259"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 30,
    "price": 4.79,
    "shipmentAddress": "2320 Urna Road",
    "zipCode": "1003"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 67,
    "price": 5.75,
    "shipmentAddress": "499-7950 Pede. St.",
    "zipCode": "5713"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 124,
    "price": 4.46,
    "shipmentAddress": "Ap #528-3178 Posuere Street",
    "zipCode": "78543"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 106,
    "price": 4.6,
    "shipmentAddress": "606-5793 Tincidunt, Road",
    "zipCode": "12683"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Pineapple",
    "quantity": 200,
    "price": 5.05,
    "shipmentAddress": "P.O. Box 294, 4993 Odio Rd.",
    "zipCode": "R2M 0L0"
  },
  {
    "orderType": "E",
    "orderItemName": "Kiwi",
    "quantity": 31,
    "price": 6.14,
    "shipmentAddress": "P.O. Box 128, 887 Vel, Ave",
    "zipCode": "96901"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 23,
    "price": 1.51,
    "shipmentAddress": "676-6530 Fringilla Rd.",
    "zipCode": "S2P 0L5"
  },
  {
    "orderType": "E",
    "orderItemName": "Pomegranate",
    "quantity": 124,
    "price": 8.9,
    "shipmentAddress": "P.O. Box 675, 4520 Tristique Ave",
    "zipCode": "76947"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 101,
    "price": 1.19,
    "shipmentAddress": "P.O. Box 578, 3911 Imperdiet Rd.",
    "zipCode": "20514"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 11,
    "price": 3.46,
    "shipmentAddress": "Ap #572-4695 Lorem St.",
    "zipCode": "7342"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 131,
    "price": 9.16,
    "shipmentAddress": "P.O. Box 126, 3866 Sed Road",
    "zipCode": "35092"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 50,
    "price": 8.95,
    "shipmentAddress": "Ap #515-3084 Quam St.",
    "zipCode": "50711"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Ale",
    "quantity": 125,
    "price": 5.99,
    "shipmentAddress": "7455 Morbi Road",
    "zipCode": "578848"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 196,
    "price": 3.34,
    "shipmentAddress": "Ap #595-1170 Lorem, Rd.",
    "zipCode": "41909"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 16,
    "price": 2.65,
    "shipmentAddress": "P.O. Box 340, 5145 Nulla. Rd.",
    "zipCode": "42705"
  },
  {
    "orderType": "E",
    "orderItemName": "Papaya",
    "quantity": 139,
    "price": 0.68,
    "shipmentAddress": "484-5584 Habitant Rd.",
    "zipCode": "1405"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 184,
    "price": 7.94,
    "shipmentAddress": "514-6546 Donec St.",
    "zipCode": "1608"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 69,
    "price": 5.03,
    "shipmentAddress": "636 Arcu. Road",
    "zipCode": "1150"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 39,
    "price": 3.17,
    "shipmentAddress": "P.O. Box 678, 1692 Fringilla Street",
    "zipCode": "2062"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 164,
    "price": 8.57,
    "shipmentAddress": "P.O. Box 706, 5691 Elit. Av.",
    "zipCode": "84844"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 19,
    "price": 2.43,
    "shipmentAddress": "5158 Eu Street",
    "zipCode": "5076"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 140,
    "price": 7.11,
    "shipmentAddress": "P.O. Box 665, 2244 Suscipit Ave",
    "zipCode": "1469 FT"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 196,
    "price": 4.14,
    "shipmentAddress": "Ap #555-8008 Egestas. Rd.",
    "zipCode": "2867"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha Irish Cream",
    "quantity": 87,
    "price": 5.95,
    "shipmentAddress": "164-4328 Erat Road",
    "zipCode": "4273 FT"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 45,
    "price": 7.78,
    "shipmentAddress": "P.O. Box 507, 3757 Nam St.",
    "zipCode": "N7Z 8N8"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 160,
    "price": 4.68,
    "shipmentAddress": "7987 Diam Avenue",
    "zipCode": "47781"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 21,
    "price": 2.56,
    "shipmentAddress": "2778 Ante. Ave",
    "zipCode": "8550"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 32,
    "price": 9.91,
    "shipmentAddress": "P.O. Box 266, 2749 Lectus. Street",
    "zipCode": "96673"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 170,
    "price": 5.1,
    "shipmentAddress": "653-9898 Neque. Av.",
    "zipCode": "6958"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 53,
    "price": 4.64,
    "shipmentAddress": "P.O. Box 924, 8573 Ac Avenue",
    "zipCode": "0993 GC"
  },
  {
    "orderType": "E",
    "orderItemName": "Spearmint",
    "quantity": 137,
    "price": 9.83,
    "shipmentAddress": "7293 Cursus Street",
    "zipCode": "10812"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 102,
    "price": 7.37,
    "shipmentAddress": "Ap #253-6584 Aliquet Rd.",
    "zipCode": "639050"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 127,
    "price": 0.99,
    "shipmentAddress": "Ap #849-6582 Ut Rd.",
    "zipCode": "97533"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 25,
    "price": 3.09,
    "shipmentAddress": "869 Varius Av.",
    "zipCode": "98-726"
  },
  {
    "orderType": "E",
    "orderItemName": "Margarita",
    "quantity": 60,
    "price": 9.8,
    "shipmentAddress": "Ap #192-954 Donec Ave",
    "zipCode": "K3Z 3S8"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry",
    "quantity": 153,
    "price": 4.72,
    "shipmentAddress": "Ap #804-5597 Ipsum. St.",
    "zipCode": "64-596"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 137,
    "price": 5.97,
    "shipmentAddress": "5464 Neque Rd.",
    "zipCode": "97751-455"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 49,
    "price": 1.73,
    "shipmentAddress": "4421 Ac Street",
    "zipCode": "091236"
  },
  {
    "orderType": "E",
    "orderItemName": "Sour",
    "quantity": 72,
    "price": 5.16,
    "shipmentAddress": "369-5988 Integer Avenue",
    "zipCode": "78061"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 21,
    "price": 8.31,
    "shipmentAddress": "602-6746 Cursus Rd.",
    "zipCode": "59140"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 193,
    "price": 6.26,
    "shipmentAddress": "142-8683 Duis Av.",
    "zipCode": "86668"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 21,
    "price": 6.86,
    "shipmentAddress": "5914 Tempor Ave",
    "zipCode": "63587"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 86,
    "price": 9.89,
    "shipmentAddress": "210-843 Eu Avenue",
    "zipCode": "226657"
  },
  {
    "orderType": "E",
    "orderItemName": "Pina Colada",
    "quantity": 72,
    "price": 9.11,
    "shipmentAddress": "P.O. Box 638, 9206 Interdum. Avenue",
    "zipCode": "37815"
  },
  {
    "orderType": "E",
    "orderItemName": "Sarsaparilla",
    "quantity": 90,
    "price": 0.6,
    "shipmentAddress": "P.O. Box 783, 6939 Malesuada Street",
    "zipCode": "978620"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 73,
    "price": 3.83,
    "shipmentAddress": "Ap #448-1086 Fusce Av.",
    "zipCode": "1719"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 84,
    "price": 2.81,
    "shipmentAddress": "9896 Malesuada Road",
    "zipCode": "05-400"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 110,
    "price": 1.33,
    "shipmentAddress": "836-9333 Luctus Ave",
    "zipCode": "20211"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 149,
    "price": 8.29,
    "shipmentAddress": "P.O. Box 388, 1296 Mauris Rd.",
    "zipCode": "829393"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Pineapple",
    "quantity": 165,
    "price": 8.27,
    "shipmentAddress": "3079 Sit St.",
    "zipCode": "9298"
  },
  {
    "orderType": "E",
    "orderItemName": "Pink Lemonade",
    "quantity": 102,
    "price": 7.99,
    "shipmentAddress": "148 Eget, Road",
    "zipCode": "926860"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon",
    "quantity": 36,
    "price": 2.11,
    "shipmentAddress": "5388 Lorem Rd.",
    "zipCode": "9210"
  },
  {
    "orderType": "E",
    "orderItemName": "Butterscotch",
    "quantity": 136,
    "price": 4.53,
    "shipmentAddress": "Ap #261-7442 Nullam Road",
    "zipCode": "10739"
  },
  {
    "orderType": "E",
    "orderItemName": "Cotton Candy",
    "quantity": 186,
    "price": 1.59,
    "shipmentAddress": "Ap #272-5089 Hendrerit Av.",
    "zipCode": "0557 HO"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 84,
    "price": 5.73,
    "shipmentAddress": "Ap #181-8745 Luctus St.",
    "zipCode": "29602"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 180,
    "price": 9.48,
    "shipmentAddress": "Ap #855-9090 Enim. Street",
    "zipCode": "02261"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 154,
    "price": 2.05,
    "shipmentAddress": "412-6587 Mollis. St.",
    "zipCode": "84-475"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 116,
    "price": 9.4,
    "shipmentAddress": "P.O. Box 375, 3296 At, St.",
    "zipCode": "VD71 5HH"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 18,
    "price": 9.06,
    "shipmentAddress": "Ap #732-7475 Aliquam Street",
    "zipCode": "43447"
  },
  {
    "orderType": "E",
    "orderItemName": "Cranberry",
    "quantity": 95,
    "price": 2.79,
    "shipmentAddress": "Ap #154-5329 Nisi Avenue",
    "zipCode": "5051"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 50,
    "price": 2.77,
    "shipmentAddress": "126-8450 Metus Av.",
    "zipCode": "Y5T 6M1"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 121,
    "price": 4.57,
    "shipmentAddress": "Ap #936-8317 Montes, Av.",
    "zipCode": "42-794"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 74,
    "price": 0.33,
    "shipmentAddress": "Ap #665-8494 Ac, St.",
    "zipCode": "985872"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 158,
    "price": 0.25,
    "shipmentAddress": "8425 Nunc Road",
    "zipCode": "SY4 1GT"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango",
    "quantity": 190,
    "price": 1.4,
    "shipmentAddress": "2375 Nec, Avenue",
    "zipCode": "29390"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Almond",
    "quantity": 161,
    "price": 2.78,
    "shipmentAddress": "Ap #188-2434 Sagittis Rd.",
    "zipCode": "2665"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 117,
    "price": 5.06,
    "shipmentAddress": "Ap #297-2740 Aliquam Street",
    "zipCode": "LF81 1ZB"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate",
    "quantity": 173,
    "price": 8.07,
    "shipmentAddress": "245-4946 Velit Road",
    "zipCode": "20241"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 112,
    "price": 3.02,
    "shipmentAddress": "107-5773 Curae; St.",
    "zipCode": "6158"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemonade",
    "quantity": 185,
    "price": 4.61,
    "shipmentAddress": "5695 Purus. St.",
    "zipCode": "75062"
  },
  {
    "orderType": "E",
    "orderItemName": "Cranberry",
    "quantity": 31,
    "price": 7.87,
    "shipmentAddress": "P.O. Box 119, 9263 Sed Rd.",
    "zipCode": "662479"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 43,
    "price": 5.68,
    "shipmentAddress": "Ap #186-3978 Phasellus Rd.",
    "zipCode": "11954"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate",
    "quantity": 32,
    "price": 7.75,
    "shipmentAddress": "P.O. Box 573, 2683 Congue. Ave",
    "zipCode": "1690"
  },
  {
    "orderType": "E",
    "orderItemName": "Punch",
    "quantity": 102,
    "price": 1.35,
    "shipmentAddress": "449-2421 Nec, Av.",
    "zipCode": "43790"
  },
  {
    "orderType": "E",
    "orderItemName": "Huckleberry",
    "quantity": 161,
    "price": 8.5,
    "shipmentAddress": "480-5013 Quam. Road",
    "zipCode": "2531"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 79,
    "price": 6.35,
    "shipmentAddress": "P.O. Box 398, 6936 Tempus St.",
    "zipCode": "98-460"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 165,
    "price": 4.36,
    "shipmentAddress": "102-2535 Morbi Avenue",
    "zipCode": "90578"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 97,
    "price": 0.06,
    "shipmentAddress": "9395 Nisi. Street",
    "zipCode": "57401"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 68,
    "price": 0.09,
    "shipmentAddress": "283-3174 Fermentum Av.",
    "zipCode": "66289-471"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 106,
    "price": 6.05,
    "shipmentAddress": "5220 Augue St.",
    "zipCode": "65260"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 28,
    "price": 0.41,
    "shipmentAddress": "P.O. Box 870, 7081 Elit Ave",
    "zipCode": "65732"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 80,
    "price": 5.23,
    "shipmentAddress": "8921 Scelerisque Street",
    "zipCode": "3600 FX"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 162,
    "price": 7.09,
    "shipmentAddress": "7566 Cras Rd.",
    "zipCode": "7696"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 195,
    "price": 8.76,
    "shipmentAddress": "806-1641 Ligula. Av.",
    "zipCode": "6386"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 163,
    "price": 5.62,
    "shipmentAddress": "8952 Sit Av.",
    "zipCode": "04980"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 145,
    "price": 6.71,
    "shipmentAddress": "3819 Pellentesque Road",
    "zipCode": "09871"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemonade",
    "quantity": 145,
    "price": 8.58,
    "shipmentAddress": "271-9059 At Rd.",
    "zipCode": "40305"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 78,
    "price": 3.85,
    "shipmentAddress": "Ap #214-4486 Tristique Rd.",
    "zipCode": "57422-816"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 26,
    "price": 2.11,
    "shipmentAddress": "247-7484 Nullam Avenue",
    "zipCode": "15904"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 126,
    "price": 8.12,
    "shipmentAddress": "Ap #137-1712 Arcu Avenue",
    "zipCode": "736590"
  },
  {
    "orderType": "E",
    "orderItemName": "Acai Berry",
    "quantity": 182,
    "price": 0.56,
    "shipmentAddress": "344-8558 Neque St.",
    "zipCode": "61764"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 182,
    "price": 2.02,
    "shipmentAddress": "P.O. Box 619, 3490 At Rd.",
    "zipCode": "51604-589"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 134,
    "price": 2.98,
    "shipmentAddress": "Ap #485-3719 Magna. Rd.",
    "zipCode": "26496-281"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 165,
    "price": 3.19,
    "shipmentAddress": "732-4000 Mauris. Rd.",
    "zipCode": "N3A 7M0"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 15,
    "price": 7.08,
    "shipmentAddress": "P.O. Box 115, 4326 Ut Av.",
    "zipCode": "10605"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 159,
    "price": 3.44,
    "shipmentAddress": "Ap #828-1656 Et St.",
    "zipCode": "503405"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 42,
    "price": 4.91,
    "shipmentAddress": "Ap #683-4456 Hendrerit. Road",
    "zipCode": "M6Z 8K1"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Walnut",
    "quantity": 173,
    "price": 1.77,
    "shipmentAddress": "9694 Odio St.",
    "zipCode": "65032"
  },
  {
    "orderType": "E",
    "orderItemName": "Graham Cracker",
    "quantity": 172,
    "price": 6.61,
    "shipmentAddress": "P.O. Box 384, 6499 Eu, Avenue",
    "zipCode": "7520"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Toffee",
    "quantity": 27,
    "price": 6.65,
    "shipmentAddress": "Ap #409-6817 Ultrices. Rd.",
    "zipCode": "4378"
  },
  {
    "orderType": "E",
    "orderItemName": "Gingersnap",
    "quantity": 55,
    "price": 2.12,
    "shipmentAddress": "3164 Proin St.",
    "zipCode": "5436 PD"
  },
  {
    "orderType": "E",
    "orderItemName": "Cola",
    "quantity": 155,
    "price": 8.01,
    "shipmentAddress": "Ap #300-3615 Mauris. St.",
    "zipCode": "5145"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 125,
    "price": 4.75,
    "shipmentAddress": "P.O. Box 781, 3278 Ut, Rd.",
    "zipCode": "9262"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 145,
    "price": 1.45,
    "shipmentAddress": "P.O. Box 263, 4557 Laoreet Rd.",
    "zipCode": "6879"
  },
  {
    "orderType": "E",
    "orderItemName": "Cranberry",
    "quantity": 121,
    "price": 8.4,
    "shipmentAddress": "318-6624 Fringilla Road",
    "zipCode": "45762"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 12,
    "price": 6.06,
    "shipmentAddress": "6495 Lectus, Rd.",
    "zipCode": "620400"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 54,
    "price": 2.4,
    "shipmentAddress": "P.O. Box 777, 4384 Magna Ave",
    "zipCode": "21077"
  },
  {
    "orderType": "E",
    "orderItemName": "Pina Colada",
    "quantity": 134,
    "price": 5.78,
    "shipmentAddress": "585-2257 Vestibulum Avenue",
    "zipCode": "47408"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 196,
    "price": 3.66,
    "shipmentAddress": "4066 Integer Ave",
    "zipCode": "3811 YB"
  },
  {
    "orderType": "E",
    "orderItemName": "Cola",
    "quantity": 55,
    "price": 9.92,
    "shipmentAddress": "P.O. Box 209, 3173 Tempor Street",
    "zipCode": "26-459"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 177,
    "price": 5.7,
    "shipmentAddress": "115-2623 Dui St.",
    "zipCode": "697389"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemonade",
    "quantity": 88,
    "price": 3.66,
    "shipmentAddress": "P.O. Box 316, 4896 Cras Avenue",
    "zipCode": "4632"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 25,
    "price": 7.86,
    "shipmentAddress": "Ap #955-4370 Elementum Av.",
    "zipCode": "32-990"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 14,
    "price": 4.3,
    "shipmentAddress": "894-1548 Ipsum Road",
    "zipCode": "7268"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 104,
    "price": 0.65,
    "shipmentAddress": "635-7304 Est Rd.",
    "zipCode": "56519"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 131,
    "price": 9.14,
    "shipmentAddress": "4147 Et Av.",
    "zipCode": "053489"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 102,
    "price": 3.15,
    "shipmentAddress": "Ap #391-2217 At Rd.",
    "zipCode": "8835"
  },
  {
    "orderType": "E",
    "orderItemName": "Sarsaparilla",
    "quantity": 60,
    "price": 2.6,
    "shipmentAddress": "Ap #262-5147 Non, Ave",
    "zipCode": "48-426"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 171,
    "price": 4.45,
    "shipmentAddress": "P.O. Box 400, 4881 Sociis Rd.",
    "zipCode": "753233"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 57,
    "price": 9.9,
    "shipmentAddress": "569 Dolor Ave",
    "zipCode": "98542"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 174,
    "price": 0.5,
    "shipmentAddress": "7084 Diam. Ave",
    "zipCode": "5730"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Lime",
    "quantity": 155,
    "price": 1.32,
    "shipmentAddress": "6329 Justo Rd.",
    "zipCode": "8418"
  },
  {
    "orderType": "E",
    "orderItemName": "Whipped Cream",
    "quantity": 39,
    "price": 2.3,
    "shipmentAddress": "P.O. Box 523, 4804 Gravida Rd.",
    "zipCode": "17901"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 65,
    "price": 1.3,
    "shipmentAddress": "Ap #672-6709 Nunc Road",
    "zipCode": "2501 MH"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 107,
    "price": 1.7,
    "shipmentAddress": "9371 Suscipit Ave",
    "zipCode": "590191"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan",
    "quantity": 200,
    "price": 8.87,
    "shipmentAddress": "Ap #948-5608 Magnis St.",
    "zipCode": "56087"
  },
  {
    "orderType": "E",
    "orderItemName": "Doughnut",
    "quantity": 182,
    "price": 3.18,
    "shipmentAddress": "956-7780 Tristique Av.",
    "zipCode": "6201"
  },
  {
    "orderType": "E",
    "orderItemName": "Tropical Punch",
    "quantity": 142,
    "price": 4.82,
    "shipmentAddress": "P.O. Box 154, 1716 Tellus St.",
    "zipCode": "6692 GG"
  },
  {
    "orderType": "E",
    "orderItemName": "Key Lime",
    "quantity": 188,
    "price": 2.3,
    "shipmentAddress": "Ap #386-2178 Augue Avenue",
    "zipCode": "9555"
  },
  {
    "orderType": "E",
    "orderItemName": "Huckleberry",
    "quantity": 53,
    "price": 8.94,
    "shipmentAddress": "188-3228 Lobortis St.",
    "zipCode": "292658"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 175,
    "price": 8.73,
    "shipmentAddress": "938-6277 Mi Ave",
    "zipCode": "36402"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 175,
    "price": 5.32,
    "shipmentAddress": "Ap #590-6177 Ipsum St.",
    "zipCode": "5483"
  },
  {
    "orderType": "E",
    "orderItemName": "Wintergreen",
    "quantity": 141,
    "price": 2.19,
    "shipmentAddress": "Ap #910-4206 Tellus, Rd.",
    "zipCode": "096252"
  },
  {
    "orderType": "E",
    "orderItemName": "Doughnut",
    "quantity": 114,
    "price": 8.78,
    "shipmentAddress": "4181 Lectus. St.",
    "zipCode": "50717"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 48,
    "price": 8.77,
    "shipmentAddress": "157-7141 Tincidunt. Road",
    "zipCode": "76550"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 129,
    "price": 8.03,
    "shipmentAddress": "P.O. Box 340, 6874 Auctor Street",
    "zipCode": "84183"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 168,
    "price": 0.64,
    "shipmentAddress": "484-2736 Erat Av.",
    "zipCode": "5991"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 21,
    "price": 7.44,
    "shipmentAddress": "Ap #865-3325 Vel Rd.",
    "zipCode": "258844"
  },
  {
    "orderType": "E",
    "orderItemName": "Sour",
    "quantity": 121,
    "price": 4.42,
    "shipmentAddress": "P.O. Box 985, 8863 Phasellus Av.",
    "zipCode": "74202"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 193,
    "price": 0.87,
    "shipmentAddress": "P.O. Box 117, 8656 Tortor Rd.",
    "zipCode": "69-783"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 116,
    "price": 3.74,
    "shipmentAddress": "3391 Nec St.",
    "zipCode": "OB5 3LJ"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango",
    "quantity": 14,
    "price": 0.43,
    "shipmentAddress": "P.O. Box 977, 339 Malesuada Rd.",
    "zipCode": "30621"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 145,
    "price": 1.23,
    "shipmentAddress": "P.O. Box 305, 8664 Turpis Rd.",
    "zipCode": "72434"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 130,
    "price": 8.35,
    "shipmentAddress": "607-897 Et Street",
    "zipCode": "A39 2RL"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel Cream",
    "quantity": 51,
    "price": 1.32,
    "shipmentAddress": "5111 Aliquet St.",
    "zipCode": "5262"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 93,
    "price": 5.78,
    "shipmentAddress": "Ap #796-6863 Nulla Avenue",
    "zipCode": "7648"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 180,
    "price": 8.13,
    "shipmentAddress": "Ap #127-5265 Est Rd.",
    "zipCode": "AF77 3KH"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 48,
    "price": 8.98,
    "shipmentAddress": "889 Suspendisse St.",
    "zipCode": "386399"
  },
  {
    "orderType": "E",
    "orderItemName": "Spearmint",
    "quantity": 156,
    "price": 1.99,
    "shipmentAddress": "8659 Tellus Street",
    "zipCode": "7636"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 109,
    "price": 8.16,
    "shipmentAddress": "Ap #775-7868 Suscipit, Street",
    "zipCode": "44120"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 58,
    "price": 6,
    "shipmentAddress": "4067 Elit Rd.",
    "zipCode": "H7X 2L6"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 101,
    "price": 9.71,
    "shipmentAddress": "P.O. Box 480, 6815 Vel, Road",
    "zipCode": "67572"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 121,
    "price": 9.74,
    "shipmentAddress": "P.O. Box 603, 2576 Id Street",
    "zipCode": "61906"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 132,
    "price": 3.19,
    "shipmentAddress": "P.O. Box 560, 3381 Ridiculus St.",
    "zipCode": "44794"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 61,
    "price": 8.17,
    "shipmentAddress": "Ap #385-495 Suspendisse St.",
    "zipCode": "114261"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 72,
    "price": 3.65,
    "shipmentAddress": "P.O. Box 352, 5437 Montes, Ave",
    "zipCode": "20252"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Cream",
    "quantity": 121,
    "price": 6.42,
    "shipmentAddress": "P.O. Box 337, 2097 Phasellus Av.",
    "zipCode": "R7 9HU"
  },
  {
    "orderType": "E",
    "orderItemName": "Cream Soda",
    "quantity": 89,
    "price": 8.67,
    "shipmentAddress": "9277 Cursus Road",
    "zipCode": "5955 KN"
  },
  {
    "orderType": "E",
    "orderItemName": "Fuzzy Navel",
    "quantity": 161,
    "price": 6.36,
    "shipmentAddress": "9498 Nisi St.",
    "zipCode": "4673"
  },
  {
    "orderType": "E",
    "orderItemName": "Cheesecake",
    "quantity": 27,
    "price": 1.87,
    "shipmentAddress": "465-3177 In, St.",
    "zipCode": "921697"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 41,
    "price": 2.59,
    "shipmentAddress": "Ap #598-6067 Hendrerit. Rd.",
    "zipCode": "457427"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 35,
    "price": 7.23,
    "shipmentAddress": "934-5586 Tincidunt Rd.",
    "zipCode": "87700"
  },
  {
    "orderType": "E",
    "orderItemName": "Sour",
    "quantity": 24,
    "price": 4.12,
    "shipmentAddress": "Ap #371-8023 Eu Rd.",
    "zipCode": "7160"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 196,
    "price": 0.72,
    "shipmentAddress": "P.O. Box 129, 7692 Proin Av.",
    "zipCode": "7549"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 51,
    "price": 7.7,
    "shipmentAddress": "P.O. Box 579, 6007 Ante. Road",
    "zipCode": "7858"
  },
  {
    "orderType": "E",
    "orderItemName": "Grand Mariner",
    "quantity": 89,
    "price": 3.59,
    "shipmentAddress": "Ap #567-7347 Odio Rd.",
    "zipCode": "5361"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 147,
    "price": 8.57,
    "shipmentAddress": "P.O. Box 303, 2577 Aenean St.",
    "zipCode": "40-643"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 189,
    "price": 8.85,
    "shipmentAddress": "P.O. Box 480, 7940 Etiam Road",
    "zipCode": "6258 YT"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Brandy",
    "quantity": 50,
    "price": 2.55,
    "shipmentAddress": "P.O. Box 730, 8356 Malesuada Avenue",
    "zipCode": "66-395"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 166,
    "price": 5.88,
    "shipmentAddress": "2731 Malesuada Street",
    "zipCode": "08059"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 34,
    "price": 2.38,
    "shipmentAddress": "Ap #861-9051 Non Av.",
    "zipCode": "G6A 0E6"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 113,
    "price": 7.75,
    "shipmentAddress": "1840 Ligula. St.",
    "zipCode": "6060"
  },
  {
    "orderType": "E",
    "orderItemName": "Blackberry",
    "quantity": 160,
    "price": 5.35,
    "shipmentAddress": "672 A Ave",
    "zipCode": "KS6 8ZC"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 89,
    "price": 9.55,
    "shipmentAddress": "P.O. Box 266, 7627 In Ave",
    "zipCode": "6760 QH"
  },
  {
    "orderType": "E",
    "orderItemName": "Melon Kiwi",
    "quantity": 39,
    "price": 6.44,
    "shipmentAddress": "P.O. Box 236, 6468 Dapibus Avenue",
    "zipCode": "70419"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 184,
    "price": 5.19,
    "shipmentAddress": "Ap #815-911 Posuere Road",
    "zipCode": "K8L 9G4"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 113,
    "price": 4.83,
    "shipmentAddress": "Ap #247-6589 Nec Ave",
    "zipCode": "49-776"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 164,
    "price": 2.11,
    "shipmentAddress": "6033 Egestas Avenue",
    "zipCode": "45125"
  },
  {
    "orderType": "E",
    "orderItemName": "Banana",
    "quantity": 25,
    "price": 8.17,
    "shipmentAddress": "328-9715 Diam Avenue",
    "zipCode": "568646"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Walnut",
    "quantity": 74,
    "price": 5.56,
    "shipmentAddress": "191-6712 Ornare, Avenue",
    "zipCode": "68-263"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 137,
    "price": 4.49,
    "shipmentAddress": "967-456 Donec Av.",
    "zipCode": "86878"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Walnut",
    "quantity": 164,
    "price": 9.23,
    "shipmentAddress": "Ap #795-4009 Ipsum Street",
    "zipCode": "W60 1DF"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 78,
    "price": 5.75,
    "shipmentAddress": "577-9037 Curabitur Road",
    "zipCode": "762635"
  },
  {
    "orderType": "E",
    "orderItemName": "Grape",
    "quantity": 167,
    "price": 3.76,
    "shipmentAddress": "Ap #758-8169 In Street",
    "zipCode": "94062"
  },
  {
    "orderType": "E",
    "orderItemName": "Bavarian Cream",
    "quantity": 159,
    "price": 4.53,
    "shipmentAddress": "Ap #998-4299 Semper Street",
    "zipCode": "N26 2VU"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 49,
    "price": 6.71,
    "shipmentAddress": "P.O. Box 227, 2555 Massa. Road",
    "zipCode": "391257"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 52,
    "price": 7.62,
    "shipmentAddress": "Ap #739-8383 Vestibulum Street",
    "zipCode": "39-909"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 174,
    "price": 5.08,
    "shipmentAddress": "P.O. Box 141, 6473 Nisi St.",
    "zipCode": "828263"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 99,
    "price": 2.79,
    "shipmentAddress": "230-6119 Arcu. Rd.",
    "zipCode": "5748"
  },
  {
    "orderType": "E",
    "orderItemName": "Pistachio",
    "quantity": 180,
    "price": 1.95,
    "shipmentAddress": "Ap #644-7559 Duis Road",
    "zipCode": "02908-963"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 58,
    "price": 1.17,
    "shipmentAddress": "Ap #461-5719 Montes, Rd.",
    "zipCode": "99349"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel",
    "quantity": 140,
    "price": 4.85,
    "shipmentAddress": "P.O. Box 372, 6555 Rutrum, Ave",
    "zipCode": "354292"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 13,
    "price": 9.73,
    "shipmentAddress": "822-7103 Dolor. Av.",
    "zipCode": "823339"
  },
  {
    "orderType": "E",
    "orderItemName": "Tequila",
    "quantity": 91,
    "price": 3.79,
    "shipmentAddress": "968-1000 Lorem Rd.",
    "zipCode": "74407"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 40,
    "price": 0.93,
    "shipmentAddress": "P.O. Box 321, 5334 Tincidunt, Ave",
    "zipCode": "6093"
  },
  {
    "orderType": "E",
    "orderItemName": "Tonic",
    "quantity": 68,
    "price": 1.36,
    "shipmentAddress": "P.O. Box 795, 496 Egestas Ave",
    "zipCode": "704567"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Almond",
    "quantity": 63,
    "price": 5.24,
    "shipmentAddress": "1801 Magnis St.",
    "zipCode": "5952"
  },
  {
    "orderType": "E",
    "orderItemName": "Strawberry Kiwi",
    "quantity": 51,
    "price": 3.71,
    "shipmentAddress": "203-8218 Molestie Road",
    "zipCode": "50204"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 29,
    "price": 0.4,
    "shipmentAddress": "Ap #310-3620 Fringilla Rd.",
    "zipCode": "3376"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemonade",
    "quantity": 52,
    "price": 5.23,
    "shipmentAddress": "925-9879 Non Rd.",
    "zipCode": "48901"
  },
  {
    "orderType": "E",
    "orderItemName": "Smoke",
    "quantity": 174,
    "price": 2.23,
    "shipmentAddress": "965 Magnis Road",
    "zipCode": "41044"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 35,
    "price": 7.63,
    "shipmentAddress": "441-3559 Turpis Avenue",
    "zipCode": "E9L 1E4"
  },
  {
    "orderType": "E",
    "orderItemName": "Margarita",
    "quantity": 85,
    "price": 1.25,
    "shipmentAddress": "5048 Nunc Av.",
    "zipCode": "KR5 1BV"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 80,
    "price": 0,
    "shipmentAddress": "977-9202 Nec Rd.",
    "zipCode": "74753"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 198,
    "price": 6.93,
    "shipmentAddress": "Ap #285-691 Ultrices. Road",
    "zipCode": "210021"
  },
  {
    "orderType": "E",
    "orderItemName": "Toffee",
    "quantity": 100,
    "price": 8.6,
    "shipmentAddress": "Ap #366-7965 Sem Road",
    "zipCode": "82-469"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 177,
    "price": 5.37,
    "shipmentAddress": "2301 Sed Street",
    "zipCode": "61311"
  },
  {
    "orderType": "E",
    "orderItemName": "Pomegranate",
    "quantity": 57,
    "price": 2.7,
    "shipmentAddress": "154-4039 Tristique Ave",
    "zipCode": "56382"
  },
  {
    "orderType": "E",
    "orderItemName": "Sarsaparilla",
    "quantity": 152,
    "price": 8.98,
    "shipmentAddress": "4345 Non Avenue",
    "zipCode": "J6X 1J9"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 160,
    "price": 0.06,
    "shipmentAddress": "607-7126 Placerat. St.",
    "zipCode": "95270"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 157,
    "price": 1.23,
    "shipmentAddress": "2685 Nisi Av.",
    "zipCode": "K8 9JS"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 85,
    "price": 7.35,
    "shipmentAddress": "Ap #883-1163 Aliquet Rd.",
    "zipCode": "TQ1I 4HS"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 174,
    "price": 6.15,
    "shipmentAddress": "P.O. Box 982, 7588 Fermentum Avenue",
    "zipCode": "41503"
  },
  {
    "orderType": "E",
    "orderItemName": "Eucalyptus",
    "quantity": 111,
    "price": 6.94,
    "shipmentAddress": "P.O. Box 760, 5356 Commodo Rd.",
    "zipCode": "956698"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 190,
    "price": 3.86,
    "shipmentAddress": "468-6203 Pharetra. Ave",
    "zipCode": "581542"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon",
    "quantity": 68,
    "price": 4.83,
    "shipmentAddress": "Ap #779-3266 Maecenas Rd.",
    "zipCode": "394980"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 102,
    "price": 6.85,
    "shipmentAddress": "P.O. Box 600, 3438 Gravida. Av.",
    "zipCode": "20593"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 30,
    "price": 9.93,
    "shipmentAddress": "P.O. Box 186, 5343 Aliquet St.",
    "zipCode": "43428"
  },
  {
    "orderType": "E",
    "orderItemName": "Mint",
    "quantity": 200,
    "price": 8.13,
    "shipmentAddress": "6395 Tristique Av.",
    "zipCode": "05063"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 15,
    "price": 3.58,
    "shipmentAddress": "Ap #455-5904 Luctus Ave",
    "zipCode": "41396"
  },
  {
    "orderType": "E",
    "orderItemName": "Sarsaparilla",
    "quantity": 75,
    "price": 0.83,
    "shipmentAddress": "P.O. Box 263, 2134 Tempor Rd.",
    "zipCode": "91181"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 31,
    "price": 5.06,
    "shipmentAddress": "P.O. Box 594, 111 Duis Ave",
    "zipCode": "20715"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 165,
    "price": 2.61,
    "shipmentAddress": "4009 A Road",
    "zipCode": "9326 KE"
  },
  {
    "orderType": "E",
    "orderItemName": "Birch Beer",
    "quantity": 101,
    "price": 3,
    "shipmentAddress": "Ap #395-4757 Volutpat. Rd.",
    "zipCode": "A7V 9TJ"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 198,
    "price": 9.83,
    "shipmentAddress": "583-4703 Tincidunt Av.",
    "zipCode": "S5Z 6N4"
  },
  {
    "orderType": "E",
    "orderItemName": "Blackberry",
    "quantity": 14,
    "price": 3.22,
    "shipmentAddress": "8667 Iaculis Av.",
    "zipCode": "7732"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 147,
    "price": 7.6,
    "shipmentAddress": "Ap #400-6376 Enim. Ave",
    "zipCode": "10809"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 180,
    "price": 9.65,
    "shipmentAddress": "413 Risus. Avenue",
    "zipCode": "4236"
  },
  {
    "orderType": "E",
    "orderItemName": "Berry Cola",
    "quantity": 126,
    "price": 2.53,
    "shipmentAddress": "P.O. Box 951, 6929 Et Street",
    "zipCode": "752849"
  },
  {
    "orderType": "E",
    "orderItemName": "Tropical Punch",
    "quantity": 127,
    "price": 5.97,
    "shipmentAddress": "233-8063 Quisque St.",
    "zipCode": "7681 BR"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 177,
    "price": 5.14,
    "shipmentAddress": "433-6981 Placerat, Ave",
    "zipCode": "PL37 3BX"
  },
  {
    "orderType": "E",
    "orderItemName": "Tangerine",
    "quantity": 64,
    "price": 0.45,
    "shipmentAddress": "419-1622 Cum Street",
    "zipCode": "898697"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 130,
    "price": 3.11,
    "shipmentAddress": "4410 Ullamcorper, Road",
    "zipCode": "ZQ2 5YS"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Walnut",
    "quantity": 160,
    "price": 6.18,
    "shipmentAddress": "2826 Dictum. Road",
    "zipCode": "S1C 5J7"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 158,
    "price": 3.82,
    "shipmentAddress": "P.O. Box 193, 7780 Neque. Avenue",
    "zipCode": "57074-857"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 22,
    "price": 7.9,
    "shipmentAddress": "976-4535 Quis, Rd.",
    "zipCode": "927285"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 177,
    "price": 0.89,
    "shipmentAddress": "425-3639 Tempor Street",
    "zipCode": "6498"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 45,
    "price": 2.21,
    "shipmentAddress": "Ap #414-1084 Cras Ave",
    "zipCode": "1246 HU"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Whiskey",
    "quantity": 188,
    "price": 8.52,
    "shipmentAddress": "646-5786 At, Road",
    "zipCode": "9288"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 195,
    "price": 4.84,
    "shipmentAddress": "803-3873 Nibh Avenue",
    "zipCode": "0847"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 143,
    "price": 8.25,
    "shipmentAddress": "P.O. Box 346, 6157 Netus St.",
    "zipCode": "1029"
  },
  {
    "orderType": "E",
    "orderItemName": "Anise",
    "quantity": 145,
    "price": 9.01,
    "shipmentAddress": "8037 Vel Rd.",
    "zipCode": "48431"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 165,
    "price": 7.41,
    "shipmentAddress": "170 Elit. Road",
    "zipCode": "9785"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 32,
    "price": 4.35,
    "shipmentAddress": "6007 Sagittis St.",
    "zipCode": "8665"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 124,
    "price": 0.59,
    "shipmentAddress": "456-1820 Erat. Avenue",
    "zipCode": "6599 WE"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry Cola",
    "quantity": 49,
    "price": 8.81,
    "shipmentAddress": "9656 Mauris Avenue",
    "zipCode": "91220"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 191,
    "price": 7.85,
    "shipmentAddress": "5161 Ac Rd.",
    "zipCode": "02438"
  },
  {
    "orderType": "E",
    "orderItemName": "Graham Cracker",
    "quantity": 26,
    "price": 4.9,
    "shipmentAddress": "P.O. Box 761, 4189 Penatibus Street",
    "zipCode": "28067-597"
  },
  {
    "orderType": "E",
    "orderItemName": "Kettle Corn",
    "quantity": 30,
    "price": 9.91,
    "shipmentAddress": "800 Eget, Avenue",
    "zipCode": "20210"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 130,
    "price": 8.28,
    "shipmentAddress": "P.O. Box 964, 7774 Velit St.",
    "zipCode": "6919"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 185,
    "price": 7.13,
    "shipmentAddress": "P.O. Box 265, 6861 Mi Avenue",
    "zipCode": "7467"
  },
  {
    "orderType": "E",
    "orderItemName": "Blue Raspberry",
    "quantity": 124,
    "price": 1.55,
    "shipmentAddress": "605-8510 Primis St.",
    "zipCode": "07622"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 196,
    "price": 8,
    "shipmentAddress": "Ap #613-3144 Vestibulum Road",
    "zipCode": "70405"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 10,
    "price": 1.52,
    "shipmentAddress": "6196 Arcu Road",
    "zipCode": "ZV76 7TK"
  },
  {
    "orderType": "E",
    "orderItemName": "Bacon",
    "quantity": 24,
    "price": 8.36,
    "shipmentAddress": "Ap #498-4243 Nisi. Ave",
    "zipCode": "5083"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 180,
    "price": 0.32,
    "shipmentAddress": "1829 Semper Avenue",
    "zipCode": "16712"
  },
  {
    "orderType": "E",
    "orderItemName": "Prickly Pear",
    "quantity": 109,
    "price": 2.89,
    "shipmentAddress": "P.O. Box 466, 2071 Mauris, Road",
    "zipCode": "90730"
  },
  {
    "orderType": "E",
    "orderItemName": "Peanut",
    "quantity": 102,
    "price": 6.44,
    "shipmentAddress": "Ap #660-2157 Aliquet Road",
    "zipCode": "2495"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry Ginger Ale",
    "quantity": 178,
    "price": 2.17,
    "shipmentAddress": "Ap #687-1059 Nam Street",
    "zipCode": "AB3H 4IS"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 15,
    "price": 5,
    "shipmentAddress": "223-2713 Fusce Rd.",
    "zipCode": "76943"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 103,
    "price": 7.01,
    "shipmentAddress": "4116 Ipsum. Street",
    "zipCode": "42146"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 26,
    "price": 7.88,
    "shipmentAddress": "Ap #722-6823 Nulla St.",
    "zipCode": "087008"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 33,
    "price": 5.94,
    "shipmentAddress": "P.O. Box 158, 1995 Cursus, Street",
    "zipCode": "74664"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 115,
    "price": 7.83,
    "shipmentAddress": "172-917 Penatibus St.",
    "zipCode": "21374"
  },
  {
    "orderType": "E",
    "orderItemName": "Amaretto",
    "quantity": 61,
    "price": 2.3,
    "shipmentAddress": "506-5313 Elit, St.",
    "zipCode": "0024 VF"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 13,
    "price": 3.19,
    "shipmentAddress": "P.O. Box 395, 5258 Purus. Avenue",
    "zipCode": "8916"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 143,
    "price": 6.07,
    "shipmentAddress": "Ap #113-2600 Fusce Av.",
    "zipCode": "4717"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon Roll",
    "quantity": 135,
    "price": 8.77,
    "shipmentAddress": "P.O. Box 276, 9833 Euismod Rd.",
    "zipCode": "50913"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 190,
    "price": 7.12,
    "shipmentAddress": "517-2483 Semper. Ave",
    "zipCode": "952913"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 57,
    "price": 9.81,
    "shipmentAddress": "P.O. Box 978, 5121 Tempus Road",
    "zipCode": "58937"
  },
  {
    "orderType": "E",
    "orderItemName": "Amaretto",
    "quantity": 157,
    "price": 5.59,
    "shipmentAddress": "Ap #902-8870 Congue, Rd.",
    "zipCode": "61912"
  },
  {
    "orderType": "E",
    "orderItemName": "Horchata",
    "quantity": 138,
    "price": 8.21,
    "shipmentAddress": "P.O. Box 544, 344 Lorem Road",
    "zipCode": "533565"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 101,
    "price": 1.42,
    "shipmentAddress": "P.O. Box 277, 1348 Ut Av.",
    "zipCode": "084542"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 12,
    "price": 3,
    "shipmentAddress": "Ap #805-420 Cursus St.",
    "zipCode": "61914"
  },
  {
    "orderType": "E",
    "orderItemName": "Cookie Dough",
    "quantity": 162,
    "price": 7.4,
    "shipmentAddress": "9220 Tempus Rd.",
    "zipCode": "3219 NT"
  },
  {
    "orderType": "E",
    "orderItemName": "Plum",
    "quantity": 152,
    "price": 2.66,
    "shipmentAddress": "957-595 Sed St.",
    "zipCode": "4513"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel Cream",
    "quantity": 113,
    "price": 1.86,
    "shipmentAddress": "7777 Interdum. Road",
    "zipCode": "3155"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 39,
    "price": 4.16,
    "shipmentAddress": "P.O. Box 616, 5444 Non Road",
    "zipCode": "1345 PS"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 27,
    "price": 9.84,
    "shipmentAddress": "Ap #304-7629 Nunc Ave",
    "zipCode": "810231"
  },
  {
    "orderType": "E",
    "orderItemName": "Gingersnap",
    "quantity": 113,
    "price": 3.03,
    "shipmentAddress": "Ap #746-3701 Arcu. St.",
    "zipCode": "711505"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate",
    "quantity": 196,
    "price": 7.25,
    "shipmentAddress": "Ap #282-6400 Tristique Street",
    "zipCode": "7681"
  },
  {
    "orderType": "E",
    "orderItemName": "Mojito",
    "quantity": 196,
    "price": 0.78,
    "shipmentAddress": "6070 Commodo Rd.",
    "zipCode": "48410"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 151,
    "price": 0.39,
    "shipmentAddress": "Ap #116-5013 Sed St.",
    "zipCode": "12984"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 47,
    "price": 1.04,
    "shipmentAddress": "149-2143 Metus. St.",
    "zipCode": "03066-885"
  },
  {
    "orderType": "E",
    "orderItemName": "Butterscotch",
    "quantity": 155,
    "price": 0,
    "shipmentAddress": "P.O. Box 160, 9944 Arcu. Street",
    "zipCode": "63135"
  },
  {
    "orderType": "E",
    "orderItemName": "Spice",
    "quantity": 83,
    "price": 9.05,
    "shipmentAddress": "628-9145 Vitae, Avenue",
    "zipCode": "1580"
  },
  {
    "orderType": "E",
    "orderItemName": "Punch",
    "quantity": 117,
    "price": 2.38,
    "shipmentAddress": "386-6620 At Road",
    "zipCode": "72124"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 124,
    "price": 9.67,
    "shipmentAddress": "128-2240 Dolor St.",
    "zipCode": "80547"
  },
  {
    "orderType": "E",
    "orderItemName": "Ginger Lime",
    "quantity": 17,
    "price": 0.6,
    "shipmentAddress": "Ap #714-3626 Eget, Road",
    "zipCode": "40795"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 23,
    "price": 3.74,
    "shipmentAddress": "5567 Rutrum, St.",
    "zipCode": "4401"
  },
  {
    "orderType": "E",
    "orderItemName": "Pink Lemonade",
    "quantity": 35,
    "price": 9.22,
    "shipmentAddress": "886-2099 Senectus St.",
    "zipCode": "2268"
  },
  {
    "orderType": "E",
    "orderItemName": "Sassafras",
    "quantity": 32,
    "price": 7.64,
    "shipmentAddress": "Ap #318-336 Dui. Street",
    "zipCode": "88238"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 45,
    "price": 9.19,
    "shipmentAddress": "806-2737 Sed, Street",
    "zipCode": "40912"
  },
  {
    "orderType": "E",
    "orderItemName": "Pecan Roll",
    "quantity": 72,
    "price": 0.15,
    "shipmentAddress": "Ap #677-8160 Odio. Rd.",
    "zipCode": "63360"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel",
    "quantity": 31,
    "price": 8.08,
    "shipmentAddress": "999-4423 Mi. Rd.",
    "zipCode": "63735"
  },
  {
    "orderType": "E",
    "orderItemName": "Bourbon",
    "quantity": 127,
    "price": 9.64,
    "shipmentAddress": "864 Egestas. Rd.",
    "zipCode": "39271"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate",
    "quantity": 95,
    "price": 9.48,
    "shipmentAddress": "Ap #518-8308 Lacus St.",
    "zipCode": "6840 KV"
  },
  {
    "orderType": "E",
    "orderItemName": "Banana",
    "quantity": 101,
    "price": 7.57,
    "shipmentAddress": "541-3580 Facilisis Ave",
    "zipCode": "46793"
  },
  {
    "orderType": "E",
    "orderItemName": "Grand Mariner",
    "quantity": 81,
    "price": 1.15,
    "shipmentAddress": "2896 Massa Rd.",
    "zipCode": "6904"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 74,
    "price": 3.77,
    "shipmentAddress": "P.O. Box 281, 6636 Sagittis Ave",
    "zipCode": "104026"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 25,
    "price": 8.21,
    "shipmentAddress": "P.O. Box 268, 7719 Enim Rd.",
    "zipCode": "95879"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 60,
    "price": 1.06,
    "shipmentAddress": "1243 Orci Rd.",
    "zipCode": "11319"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 51,
    "price": 4.85,
    "shipmentAddress": "2241 Turpis St.",
    "zipCode": "8294"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 108,
    "price": 1.38,
    "shipmentAddress": "137-7980 Arcu. Rd.",
    "zipCode": "573937"
  },
  {
    "orderType": "E",
    "orderItemName": "Pink Lemonade",
    "quantity": 170,
    "price": 4.28,
    "shipmentAddress": "P.O. Box 933, 4865 Libero St.",
    "zipCode": "85248"
  },
  {
    "orderType": "E",
    "orderItemName": "Birch Beer",
    "quantity": 135,
    "price": 3.11,
    "shipmentAddress": "Ap #610-7644 Auctor, St.",
    "zipCode": "67678"
  },
  {
    "orderType": "E",
    "orderItemName": "Long Island Tea",
    "quantity": 86,
    "price": 5.29,
    "shipmentAddress": "630-7027 Leo. Avenue",
    "zipCode": "60-763"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 45,
    "price": 9.4,
    "shipmentAddress": "P.O. Box 180, 5750 Neque Rd.",
    "zipCode": "263261"
  },
  {
    "orderType": "E",
    "orderItemName": "Toasted Coconut",
    "quantity": 165,
    "price": 6.32,
    "shipmentAddress": "P.O. Box 960, 6380 Massa. St.",
    "zipCode": "6579"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 92,
    "price": 9.81,
    "shipmentAddress": "Ap #615-4730 Aenean St.",
    "zipCode": "5296"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mango",
    "quantity": 80,
    "price": 6.71,
    "shipmentAddress": "Ap #865-3810 Ac St.",
    "zipCode": "1253"
  },
  {
    "orderType": "E",
    "orderItemName": "Blackberry",
    "quantity": 40,
    "price": 5.34,
    "shipmentAddress": "Ap #503-8361 Arcu. Road",
    "zipCode": "97170"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Pecan",
    "quantity": 34,
    "price": 8.46,
    "shipmentAddress": "P.O. Box 347, 8491 Dui Road",
    "zipCode": "94-160"
  },
  {
    "orderType": "E",
    "orderItemName": "Blackberry",
    "quantity": 54,
    "price": 4.5,
    "shipmentAddress": "3201 Tempor St.",
    "zipCode": "84401"
  },
  {
    "orderType": "E",
    "orderItemName": "Pistachio",
    "quantity": 120,
    "price": 0.81,
    "shipmentAddress": "439-8025 Rhoncus. St.",
    "zipCode": "54146"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 94,
    "price": 5.33,
    "shipmentAddress": "536-9303 Ullamcorper, St.",
    "zipCode": "600510"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Custard",
    "quantity": 114,
    "price": 1.09,
    "shipmentAddress": "950-9963 Dolor, Av.",
    "zipCode": "43687"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry",
    "quantity": 50,
    "price": 1.37,
    "shipmentAddress": "1587 Pharetra Avenue",
    "zipCode": "660459"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 55,
    "price": 2.01,
    "shipmentAddress": "515-1996 Dictum Av.",
    "zipCode": "A2G 9N6"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 178,
    "price": 3.1,
    "shipmentAddress": "Ap #222-7369 Donec Rd.",
    "zipCode": "15111"
  },
  {
    "orderType": "E",
    "orderItemName": "Sassafras",
    "quantity": 36,
    "price": 1.83,
    "shipmentAddress": "P.O. Box 645, 2473 Suspendisse Av.",
    "zipCode": "73818"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Licorice",
    "quantity": 97,
    "price": 6.99,
    "shipmentAddress": "618-9723 Metus. Road",
    "zipCode": "BE1 6YC"
  },
  {
    "orderType": "E",
    "orderItemName": "Cake Batter",
    "quantity": 56,
    "price": 7.36,
    "shipmentAddress": "Ap #498-150 Dolor. Ave",
    "zipCode": "7368 JH"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 60,
    "price": 6.66,
    "shipmentAddress": "943-5423 Volutpat Avenue",
    "zipCode": "82941"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry Ginger Ale",
    "quantity": 110,
    "price": 3.31,
    "shipmentAddress": "Ap #969-1620 Curabitur Avenue",
    "zipCode": "7034"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 157,
    "price": 1.55,
    "shipmentAddress": "7487 Euismod Street",
    "zipCode": "12976"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 65,
    "price": 2.84,
    "shipmentAddress": "P.O. Box 440, 3983 Blandit Road",
    "zipCode": "2273"
  },
  {
    "orderType": "E",
    "orderItemName": "Graham Cracker",
    "quantity": 87,
    "price": 4.3,
    "shipmentAddress": "5721 Euismod Av.",
    "zipCode": "MO6 7CV"
  },
  {
    "orderType": "E",
    "orderItemName": "Vanilla",
    "quantity": 29,
    "price": 6.43,
    "shipmentAddress": "162-8955 Erat St.",
    "zipCode": "37618"
  },
  {
    "orderType": "E",
    "orderItemName": "Root Beer",
    "quantity": 32,
    "price": 9.08,
    "shipmentAddress": "P.O. Box 619, 4125 Nisi. St.",
    "zipCode": "M7W 3G3"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 141,
    "price": 6.29,
    "shipmentAddress": "Ap #708-9644 Turpis Rd.",
    "zipCode": "46-563"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry",
    "quantity": 36,
    "price": 3.35,
    "shipmentAddress": "456-9547 Ac Avenue",
    "zipCode": "294880"
  },
  {
    "orderType": "E",
    "orderItemName": "Quinine",
    "quantity": 155,
    "price": 2.44,
    "shipmentAddress": "Ap #571-5147 Mollis St.",
    "zipCode": "4440"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Cream",
    "quantity": 132,
    "price": 6.96,
    "shipmentAddress": "9508 Pretium Av.",
    "zipCode": "62239"
  },
  {
    "orderType": "E",
    "orderItemName": "Bubble Gum",
    "quantity": 112,
    "price": 7.23,
    "shipmentAddress": "221-5981 Cursus. Ave",
    "zipCode": "4178"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 23,
    "price": 4.07,
    "shipmentAddress": "P.O. Box 957, 8856 Fusce St.",
    "zipCode": "50203"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 180,
    "price": 4.63,
    "shipmentAddress": "766-1013 Lacinia Rd.",
    "zipCode": "46046"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 147,
    "price": 0.83,
    "shipmentAddress": "977-4959 Est Street",
    "zipCode": "7660"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 185,
    "price": 4.86,
    "shipmentAddress": "122-9458 Erat, St.",
    "zipCode": "56657"
  },
  {
    "orderType": "E",
    "orderItemName": "Pear",
    "quantity": 159,
    "price": 4.91,
    "shipmentAddress": "Ap #489-412 Egestas. Avenue",
    "zipCode": "08859-301"
  },
  {
    "orderType": "E",
    "orderItemName": "Tropical Punch",
    "quantity": 26,
    "price": 3.29,
    "shipmentAddress": "120-230 Molestie Rd.",
    "zipCode": "6805"
  },
  {
    "orderType": "E",
    "orderItemName": "Spearmint",
    "quantity": 72,
    "price": 7.98,
    "shipmentAddress": "Ap #121-8464 Risus St.",
    "zipCode": "1056"
  },
  {
    "orderType": "E",
    "orderItemName": "Wild Cherry Cream",
    "quantity": 19,
    "price": 0.8,
    "shipmentAddress": "P.O. Box 423, 4621 Nunc Ave",
    "zipCode": "14595"
  },
  {
    "orderType": "E",
    "orderItemName": "Blueberry",
    "quantity": 82,
    "price": 5.43,
    "shipmentAddress": "7198 Eu Rd.",
    "zipCode": "U94 5XC"
  },
  {
    "orderType": "E",
    "orderItemName": "Passion Fruit",
    "quantity": 54,
    "price": 5.75,
    "shipmentAddress": "Ap #291-8378 Suspendisse Ave",
    "zipCode": "7392"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 103,
    "price": 3.09,
    "shipmentAddress": "7980 Et Street",
    "zipCode": "3132"
  },
  {
    "orderType": "E",
    "orderItemName": "Chocolate Mint",
    "quantity": 129,
    "price": 6.27,
    "shipmentAddress": "P.O. Box 336, 5352 Nullam St.",
    "zipCode": "67485"
  },
  {
    "orderType": "E",
    "orderItemName": "Marshmallow",
    "quantity": 48,
    "price": 8.88,
    "shipmentAddress": "P.O. Box 946, 7328 Magnis Av.",
    "zipCode": "91907"
  },
  {
    "orderType": "E",
    "orderItemName": "Candy Corn",
    "quantity": 50,
    "price": 2.7,
    "shipmentAddress": "8232 Mauris Street",
    "zipCode": "30-169"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Almond",
    "quantity": 149,
    "price": 2.41,
    "shipmentAddress": "914-4397 Arcu. Street",
    "zipCode": "76-926"
  },
  {
    "orderType": "E",
    "orderItemName": "Tutti Frutti",
    "quantity": 178,
    "price": 2.12,
    "shipmentAddress": "455-5889 Velit St.",
    "zipCode": "9195 NY"
  },
  {
    "orderType": "E",
    "orderItemName": "Malted Milk",
    "quantity": 197,
    "price": 3.01,
    "shipmentAddress": "308-3396 Cum Rd.",
    "zipCode": "87307"
  },
  {
    "orderType": "E",
    "orderItemName": "Grapefruit",
    "quantity": 70,
    "price": 4.41,
    "shipmentAddress": "Ap #895-5532 Diam. St.",
    "zipCode": "3490"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel",
    "quantity": 33,
    "price": 5.15,
    "shipmentAddress": "Ap #916-480 Ullamcorper St.",
    "zipCode": "6366"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 15,
    "price": 0.38,
    "shipmentAddress": "P.O. Box 685, 6306 In St.",
    "zipCode": "35936"
  },
  {
    "orderType": "E",
    "orderItemName": "Plantation Punch",
    "quantity": 134,
    "price": 0.95,
    "shipmentAddress": "P.O. Box 606, 9065 Sed St.",
    "zipCode": "40140"
  },
  {
    "orderType": "E",
    "orderItemName": "Apricot",
    "quantity": 186,
    "price": 1.19,
    "shipmentAddress": "P.O. Box 710, 7787 At Ave",
    "zipCode": "K8T 3W0"
  },
  {
    "orderType": "E",
    "orderItemName": "Egg Nog",
    "quantity": 46,
    "price": 8.17,
    "shipmentAddress": "7191 Aenean Avenue",
    "zipCode": "18397-985"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter",
    "quantity": 195,
    "price": 6.79,
    "shipmentAddress": "776-754 Nascetur Av.",
    "zipCode": "522048"
  },
  {
    "orderType": "E",
    "orderItemName": "Hazelnut",
    "quantity": 34,
    "price": 4.13,
    "shipmentAddress": "P.O. Box 747, 6877 Amet Street",
    "zipCode": "964125"
  },
  {
    "orderType": "E",
    "orderItemName": "Pumpkin Pie",
    "quantity": 81,
    "price": 1.52,
    "shipmentAddress": "485-5635 Eget Rd.",
    "zipCode": "4072"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha",
    "quantity": 186,
    "price": 7.41,
    "shipmentAddress": "6493 In St.",
    "zipCode": "95671"
  },
  {
    "orderType": "E",
    "orderItemName": "Coconut",
    "quantity": 19,
    "price": 0.14,
    "shipmentAddress": "Ap #810-1585 Nunc Road",
    "zipCode": "86-822"
  },
  {
    "orderType": "E",
    "orderItemName": "Kiwi",
    "quantity": 47,
    "price": 5.77,
    "shipmentAddress": "3687 Sem Street",
    "zipCode": "C7W 4J1"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Mandarin",
    "quantity": 117,
    "price": 6.29,
    "shipmentAddress": "3305 Tellus. Road",
    "zipCode": "97958"
  },
  {
    "orderType": "E",
    "orderItemName": "Grand Mariner",
    "quantity": 60,
    "price": 1.63,
    "shipmentAddress": "6077 Proin St.",
    "zipCode": "89901"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 28,
    "price": 3.57,
    "shipmentAddress": "Ap #140-4429 Magnis Rd.",
    "zipCode": "0830"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 159,
    "price": 5.22,
    "shipmentAddress": "P.O. Box 401, 7637 Eu Rd.",
    "zipCode": "53787"
  },
  {
    "orderType": "E",
    "orderItemName": "Bacon",
    "quantity": 127,
    "price": 4.46,
    "shipmentAddress": "152-801 Nec Av.",
    "zipCode": "3306 BS"
  },
  {
    "orderType": "E",
    "orderItemName": "Creme de Menthe",
    "quantity": 80,
    "price": 9.37,
    "shipmentAddress": "Ap #824-6010 Vehicula Street",
    "zipCode": "7622"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 191,
    "price": 1.36,
    "shipmentAddress": "P.O. Box 829, 6082 Vehicula. St.",
    "zipCode": "377872"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 159,
    "price": 8.7,
    "shipmentAddress": "667-721 Lorem Ave",
    "zipCode": "4661"
  },
  {
    "orderType": "E",
    "orderItemName": "Almond",
    "quantity": 172,
    "price": 6.02,
    "shipmentAddress": "7536 Purus Av.",
    "zipCode": "8442 DL"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 12,
    "price": 1.98,
    "shipmentAddress": "676-4859 Convallis Rd.",
    "zipCode": "6711 GM"
  },
  {
    "orderType": "E",
    "orderItemName": "Cherry Cola",
    "quantity": 120,
    "price": 8.28,
    "shipmentAddress": "Ap #185-2666 Dui Rd.",
    "zipCode": "18-187"
  },
  {
    "orderType": "E",
    "orderItemName": "Carrot Cake",
    "quantity": 63,
    "price": 6.44,
    "shipmentAddress": "917-8690 Feugiat Avenue",
    "zipCode": "9844"
  },
  {
    "orderType": "E",
    "orderItemName": "Flan",
    "quantity": 108,
    "price": 0.49,
    "shipmentAddress": "Ap #723-523 Ultricies St.",
    "zipCode": "62964-965"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 28,
    "price": 9.67,
    "shipmentAddress": "7700 Consectetuer St.",
    "zipCode": "T9K 9X0"
  },
  {
    "orderType": "E",
    "orderItemName": "Fenugreek",
    "quantity": 32,
    "price": 8.18,
    "shipmentAddress": "312-8033 Lorem Road",
    "zipCode": "68243"
  },
  {
    "orderType": "E",
    "orderItemName": "Watermelon",
    "quantity": 124,
    "price": 5.62,
    "shipmentAddress": "2974 Nisl. St.",
    "zipCode": "73755"
  },
  {
    "orderType": "E",
    "orderItemName": "Cake Batter",
    "quantity": 67,
    "price": 5.21,
    "shipmentAddress": "P.O. Box 839, 5416 Porttitor Street",
    "zipCode": "19-341"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Cherry",
    "quantity": 92,
    "price": 0.24,
    "shipmentAddress": "Ap #696-9183 Et Road",
    "zipCode": "43103"
  },
  {
    "orderType": "E",
    "orderItemName": "Mixed Berry",
    "quantity": 44,
    "price": 4.85,
    "shipmentAddress": "Ap #973-7663 Hendrerit Avenue",
    "zipCode": "78430"
  },
  {
    "orderType": "E",
    "orderItemName": "Pineapple",
    "quantity": 46,
    "price": 8.45,
    "shipmentAddress": "P.O. Box 547, 5346 Non St.",
    "zipCode": "497478"
  },
  {
    "orderType": "E",
    "orderItemName": "Black Currant",
    "quantity": 59,
    "price": 7.95,
    "shipmentAddress": "7748 Aliquet. Street",
    "zipCode": "06686-667"
  },
  {
    "orderType": "E",
    "orderItemName": "Macadamia Nut",
    "quantity": 183,
    "price": 9.48,
    "shipmentAddress": "Ap #340-4588 Natoque Rd.",
    "zipCode": "42786"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 85,
    "price": 2.64,
    "shipmentAddress": "3136 Vivamus St.",
    "zipCode": "4373 LH"
  },
  {
    "orderType": "E",
    "orderItemName": "Raspberry",
    "quantity": 155,
    "price": 6,
    "shipmentAddress": "8946 Ut St.",
    "zipCode": "240813"
  },
  {
    "orderType": "E",
    "orderItemName": "Gingersnap",
    "quantity": 41,
    "price": 0.3,
    "shipmentAddress": "P.O. Box 247, 9500 Eget St.",
    "zipCode": "765109"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange",
    "quantity": 188,
    "price": 0.89,
    "shipmentAddress": "P.O. Box 710, 8022 Ultrices Rd.",
    "zipCode": "35967"
  },
  {
    "orderType": "E",
    "orderItemName": "Elderberry",
    "quantity": 121,
    "price": 5.26,
    "shipmentAddress": "361-1117 Sit Rd.",
    "zipCode": "7209"
  },
  {
    "orderType": "E",
    "orderItemName": "Berry Cola",
    "quantity": 135,
    "price": 0.31,
    "shipmentAddress": "Ap #998-5481 Nibh. St.",
    "zipCode": "527517"
  },
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 154,
    "price": 0.39,
    "shipmentAddress": "Ap #480-5365 Penatibus Rd.",
    "zipCode": "06631-926"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 134,
    "price": 1.3,
    "shipmentAddress": "737 Velit Rd.",
    "zipCode": "50964"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Cream",
    "quantity": 11,
    "price": 3.3,
    "shipmentAddress": "Ap #597-2199 Ipsum Av.",
    "zipCode": "95016"
  },
  {
    "orderType": "E",
    "orderItemName": "Peppermint",
    "quantity": 63,
    "price": 6.92,
    "shipmentAddress": "P.O. Box 637, 1040 Fringilla Rd.",
    "zipCode": "02366"
  },
  {
    "orderType": "E",
    "orderItemName": "Caramel Cream",
    "quantity": 102,
    "price": 3.82,
    "shipmentAddress": "P.O. Box 777, 9661 Lobortis Street",
    "zipCode": "690518"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Toffee",
    "quantity": 161,
    "price": 3.78,
    "shipmentAddress": "2553 Risus. Ave",
    "zipCode": "761635"
  },
  {
    "orderType": "E",
    "orderItemName": "Carmel Apple",
    "quantity": 112,
    "price": 4.92,
    "shipmentAddress": "Ap #230-293 Sem. Road",
    "zipCode": "947006"
  },
  {
    "orderType": "E",
    "orderItemName": "Cinnamon",
    "quantity": 38,
    "price": 0.07,
    "shipmentAddress": "P.O. Box 720, 624 Blandit Av.",
    "zipCode": "18-994"
  },
  {
    "orderType": "E",
    "orderItemName": "White Chocolate",
    "quantity": 170,
    "price": 2.74,
    "shipmentAddress": "P.O. Box 180, 1028 Cras St.",
    "zipCode": "09337-394"
  },
  {
    "orderType": "E",
    "orderItemName": "Coffee",
    "quantity": 140,
    "price": 4.14,
    "shipmentAddress": "657-2402 Sem Rd.",
    "zipCode": "K49 1ZL"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 161,
    "price": 6.33,
    "shipmentAddress": "7364 Sed St.",
    "zipCode": "34453"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Cream",
    "quantity": 154,
    "price": 8.24,
    "shipmentAddress": "Ap #340-3371 Facilisis Avenue",
    "zipCode": "R0G 3E9"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango Orange Pineapple",
    "quantity": 29,
    "price": 8.2,
    "shipmentAddress": "526 Fusce Road",
    "zipCode": "14042-894"
  },
  {
    "orderType": "E",
    "orderItemName": "Doughnut",
    "quantity": 186,
    "price": 6.19,
    "shipmentAddress": "Ap #548-7738 Ante. Rd.",
    "zipCode": "57725"
  },
  {
    "orderType": "E",
    "orderItemName": "Mango",
    "quantity": 154,
    "price": 7.58,
    "shipmentAddress": "638-5849 Aenean St.",
    "zipCode": "917318"
  },
  {
    "orderType": "E",
    "orderItemName": "Carmel Apple",
    "quantity": 94,
    "price": 1.2,
    "shipmentAddress": "857-6713 Curabitur Street",
    "zipCode": "30091"
  },
  {
    "orderType": "E",
    "orderItemName": "Rock and Rye",
    "quantity": 199,
    "price": 4.56,
    "shipmentAddress": "Ap #686-426 Enim Rd.",
    "zipCode": "99075"
  },
  {
    "orderType": "E",
    "orderItemName": "Orange Cream",
    "quantity": 35,
    "price": 1.74,
    "shipmentAddress": "Ap #475-4868 Cras Ave",
    "zipCode": "3293"
  },
  {
    "orderType": "E",
    "orderItemName": "Tart Lemon",
    "quantity": 62,
    "price": 4.82,
    "shipmentAddress": "P.O. Box 638, 9677 Convallis Road",
    "zipCode": "18858"
  },
  {
    "orderType": "E",
    "orderItemName": "Rum",
    "quantity": 61,
    "price": 4.48,
    "shipmentAddress": "P.O. Box 745, 4399 Neque Street",
    "zipCode": "58-636"
  },
  {
    "orderType": "E",
    "orderItemName": "Irish Cream",
    "quantity": 40,
    "price": 3.32,
    "shipmentAddress": "605-3043 Faucibus. Av.",
    "zipCode": "41608"
  },
  {
    "orderType": "E",
    "orderItemName": "Maple",
    "quantity": 118,
    "price": 0.45,
    "shipmentAddress": "329-6600 Vivamus St.",
    "zipCode": "30774"
  },
  {
    "orderType": "E",
    "orderItemName": "Grand Mariner",
    "quantity": 169,
    "price": 4.88,
    "shipmentAddress": "P.O. Box 252, 3637 Dui Ave",
    "zipCode": "6137"
  },
  {
    "orderType": "E",
    "orderItemName": "Honey",
    "quantity": 186,
    "price": 9.62,
    "shipmentAddress": "600 Non St.",
    "zipCode": "750442"
  },
  {
    "orderType": "E",
    "orderItemName": "Butter Rum",
    "quantity": 96,
    "price": 1,
    "shipmentAddress": "Ap #810-2955 Sit Av.",
    "zipCode": "25091"
  },
  {
    "orderType": "E",
    "orderItemName": "Tropical Punch",
    "quantity": 110,
    "price": 2.78,
    "shipmentAddress": "4660 Sit Av.",
    "zipCode": "5165"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Lime",
    "quantity": 78,
    "price": 4.65,
    "shipmentAddress": "1313 Aenean Street",
    "zipCode": "44645"
  },
  {
    "orderType": "E",
    "orderItemName": "Apple",
    "quantity": 131,
    "price": 4.88,
    "shipmentAddress": "Ap #255-1775 Est, Avenue",
    "zipCode": "69773-309"
  },
  {
    "orderType": "E",
    "orderItemName": "Spearmint",
    "quantity": 26,
    "price": 4.69,
    "shipmentAddress": "Ap #866-7011 Sagittis. Street",
    "zipCode": "5702"
  },
  {
    "orderType": "E",
    "orderItemName": "Mocha Irish Cream",
    "quantity": 168,
    "price": 4.3,
    "shipmentAddress": "P.O. Box 511, 4483 Ornare Rd.",
    "zipCode": "C5X 6L8"
  }
]


def generate_event():
    ret = EVENT_TEMPLATES[random.randrange(1000)]
    return ret


def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    logging.info('begin sending events')
    while True:

        producer.send(args.topic,json.dumps(generate_event()).encode() , 'cust567'.encode())
        time.sleep(args.rate)
    logging.info('end sending events')


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-openshift-python emitter')
    parser = argparse.ArgumentParser(description='emit some stuff on kafka')
    parser.add_argument(
        '--brokers',
        help='The bootstrap servers, env variable KAFKA_BROKERS',
        default='localhost:9092')
    parser.add_argument(
        '--topic',
        help='Topic to publish to, env variable KAFKA_TOPIC',
        default='event-input-stream')
    parser.add_argument(
        '--rate',
        type=int,
        help='Lines per second, env variable RATE',
        default=1)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
