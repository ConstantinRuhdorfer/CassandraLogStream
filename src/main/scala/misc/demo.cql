SELECT pagepath, COUNT(visitorid)
AS num_visitor
FROM pageviews
GROUP BY pagepath limit 10;