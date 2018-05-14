# Common regular expression patterns
URL = "(http:\/\/www\.|https:\/\/www\.|http:\/\/|https" \
        + ":\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{" \
        + "2,5}(:[0-9]{1,5})?(\/.*)?"
LEADING_SPACE = "^ +"
TRAILING_SPACE = " +$"
CONSECUTIVE_SPACE = " +"
NUMBER = "\d+"
NOT_A_WORD = "[^\w\d\s]+"
BLANKS = "_+"
