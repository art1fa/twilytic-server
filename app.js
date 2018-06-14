var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var socketio = require('socket.io');
var r = require('rethinkdb');
var q = require('q');
var _ = require('lodash');
var helmet = require('helmet')

var config = require('./config');

var utils = require('./utils/utils')

var app = express();

var port = normalizePort(process.env.PORT || '3001');
app.set('port', port);

var io = socketio.listen(app.listen(port), {log: false})

// uncomment after placing your favicon in /public
//app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
app.use(helmet());

console.log("Server started on port " + port);

let conn;

r.connect(config.database).then(c => {
  conn = c;
  return r.dbCreate(config.database.db).run(conn);
})
.then(() => {
  return q.all([
    r.tableCreate("users", {primaryKey: 'id_str'}).run(conn),
    r.tableCreate("tweets", {primaryKey: 'id_str'}).run(conn),
  ]);
})
.then(() => {
  return q.all([
    r.table("tweets").indexCreate("user_id", r.row("user")("id_str")).run(conn),
    r.table("tweets").indexCreate("created_at").run(conn),
    r.table("users").indexCreate("tags", r.row("list_tags")("text"), { multi: true }).run(conn)
  ]);
})
.error(err => {
  if (err.msg.indexOf("already exists") == -1)
    console.log(err.msg);
  if (err.msg.startsWith('Could not connect'))
    throw err.msg
})
.finally(() => {
  if (conn)
    conn.close();
})

io.on("connection", socket => {
  console.log('Socket ' +socket.id + ' connected');


  // socket.users = [];
  // //CHANGEFEED
  // r.connect(config.database)
  // .then(c => {
  //   conn1 = c;
  //   return (
  //     r.table('tweets')
  //     .changes()
  //     .run(conn1)
  //   );
  // }) 
  // .then(cursor => {
  //   cursor.each((err, item) => {
  //     if (item && item.new_val) {
  //       let user_id = item.new_val.user.id_str
  //       console.log('New tweet with id ' + user_id)
  //       if (socket.users.includes(item.new_val.user.id_str)) {
  //         socket.emit('update', item.new_val);
  //       }
  //     }
  //   })
  // })
  // .error(err => {
  //   console.log("Error:", err);
  // })

  socket.on('error', (error) => {
    console.log("Error ", error);
  });

  socket.on('req_search', search => {
    let conn;
    const promises = [];
    r.connect(config.database).then(c => {
      conn = c;
      if (search.usergroups.length > 0) {
        var query = r.table('users')
        .getAll(r.args(search.usergroups), {index: 'tags'})
      
        query = query        
          .getField('id_str')
          .coerceTo('array')

        query = query
          .run(conn);

        promises.push(query);
      } else {
        promises.push([]);
      }

      if (search.keywords.length > 0 ) {
        const regex = `(?i)(\\b${search.keywords.join('\\b|\\b')}\\b)`;

        var query = r.table('tweets')
       
        //Filter retweets / replys
        if (search.options.exclude_retweets || search.options.exclude_replys) {
          query = query
          .filter(tweet => {
            if (search.options.exclude_retweets && search.options.exclude_replys) {
              return tweet.hasFields('retweeted_status').not().and(tweet('in_reply_to_status_id').eq(null));
            }
            else if (search.options.exclude_retweets) {
              return tweet.hasFields('retweeted_status').not()
            }
            else if (search.options.exclude_replys) {
              return tweet('in_reply_to_status_id').eq(null);                
            }
          })
        }

       
        // Match keyword
        query = query
        .filter(tweet => {
          if (search.options.exclude_retweets) {
            return r.branch(tweet.hasFields('extended_tweet'),
              tweet('extended_tweet')('full_text').match(regex),
              tweet('text').match(regex)     
            )
          } else {
            return r.branch(tweet.hasFields('retweeted_status'),
              r.branch(
                tweet.hasFields({'retweeted_status': 'extended_tweet'}),
                tweet('retweeted_status')('extended_tweet')('full_text').match(regex),
                tweet('retweeted_status')('text').match(regex)
              ),
              r.branch(
                tweet.hasFields('extended_tweet'),
                tweet('extended_tweet')('full_text').match(regex),
                tweet('text').match(regex)
              )
            )
          }
        })

        query = query
          .pluck('id_str', {'user': ['id_str']})
          .coerceTo('array')

        query = query
          .run(conn);

        promises.push(query);
      }
      else promises.push([]);
      return q.all(promises);
    })
    .then(result => {
      const usergroup_userids = result[0];
      const keyword_tweetids = result[1].map(tweet => tweet.id_str);
      const keyword_userids = result[1].map(tweet => tweet.user.id_str);

      if (usergroup_userids.length > 0) {
        query = r.table('tweets')
          .getAll(r.args(usergroup_userids), {index: 'user_id'})
        
        //Filter retweets / replys
        if (search.options.exclude_retweets || search.options.exclude_replys) {
          query = query
          .filter(tweet => {
            if (search.options.exclude_retweets && search.options.exclude_replys) {
              return tweet.hasFields('retweeted_status').not().and(tweet('in_reply_to_status_id').eq(null));
            }
            else if (search.options.exclude_retweets) {
              return tweet.hasFields('retweeted_status').not()
            }
            else if (search.options.exclude_replys) {
              return tweet('in_reply_to_status_id').eq(null);                
            }
          })
        }

        query = query
        .getField('id_str')
        .coerceTo('array')

        return query = query.run(conn)
        .then(result => {
          const usergroup_tweetids = result;
          
          if (search.keywords.length > 0) {
            socket.users = _.intersection(keyword_userids, usergroup_userids);
            socket.tweets = _.intersection(keyword_tweetids, usergroup_tweetids);
          }
          else {
            socket.users = _.uniq(usergroup_userids);
            socket.tweets = usergroup_tweetids;
          }
          socket.emit('done_search', {tweets: socket.tweets.length, users: socket.users.length});
        })
      }
      else{
        socket.users = _.uniq(keyword_userids);
        socket.tweets = keyword_tweetids;
        socket.emit('done_search', {tweets: socket.tweets.length, users: socket.users.length});
      }
      
    })
  })
  
  socket.on('req_stream', (params) => {
    const { mode, pageNumber } = params;
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query = r.table('tweets')

      switch (mode) {
        case 0:
          query = query
            .getAll(r.args(socket.tweets.sort(utils.alphanumSort).slice(-500))) // Make sorting a lot faster by using the tweetid information
            //.getAll(r.args(socket.tweets))
            .orderBy(r.desc('created_at'))
          break;
        case 1:
          query = query
            .getAll(r.args(socket.tweets))
            .orderBy(r.desc("favorite_count"))      
          break;      
        case 2:
          query = query
            .getAll(r.args(socket.tweets))
            .orderBy(r.desc("retweet_count"))
          break;
        default: return;
      }

      query = query
        .slice((pageNumber - 1) * 20, pageNumber * 20);
    
      query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        socket.emit('stream_recent', result);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      });
    })
  })

  socket.on('req_days_count', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query = r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        // .filter(tweet => {
        //   return r.ISO8601(tweet('created_at')).during(r.now().date().sub(30*24*60*60), r.now())
        // })
        .group(tweet => r.ISO8601(tweet('created_at')).inTimezone('+02:00').date().toISO8601())
        .count()
        .ungroup()
      query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        socket.emit('days_count_recent', result);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      })
    })
  })

  socket.on('req_hours_count', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query = r.table('tweets')
        .getAll(r.args(socket.tweets))
        .filter(tweet => {
          return r.ISO8601(tweet('created_at'))
                .during(r.now().sub(r.now().minutes().mul(60).add(23*60*60)), r.now())
        })
        .group(tweet => r.ISO8601(tweet('created_at')).inTimezone('+02:00').hours())
        .count()
        .ungroup()
      query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        socket.emit('hours_count_recent', result);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      })
    })
  })

  socket.on('req_users_follower', data => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      r.table('users')
        .getAll(r.args(socket.users))
        .orderBy(r.desc('followers_count'))
        .pluck('id_str', 'name', 'screen_name', 'verified', 'friends_count', 'followers_count', 'profile_image_url_https')
        .limit(10)
        .run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        socket.emit('users_follower', result);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      });
    })
  })

  socket.on('req_users_follower_distrib', groups => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      r.table('users')
        .getAll(r.args(socket.users))
        .getField('followers_count')
        .run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        let followerDistrib = {
          groups: Array(groups.length).fill(0),
          average: 0,
        };
        if (result.length > 0) {
          followerDistrib.groups = utils.groupBy(result, groups);
          followerDistrib.average = Math.round(result.reduce((sum, x) => sum += x) / result.length); 
        }
        socket.emit('users_follower_distrib', followerDistrib);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      });
    })
  })

  socket.on('req_users_tweet_count_overall', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query = 
      r.table('users')
        .getAll(r.args(socket.users))
        .pluck('id_str', 'name', 'screen_name', 'profile_image_url_https')
        .merge(user => {
          return { 
            'tweets_count': r.table('tweets')
                            .getAll(user('id_str'), {index: 'user_id'})
                            .filter(tweet => {
                              if (search.options.exclude_retweets && search.options.exclude_replys) {
                                return tweet.hasFields('retweeted_status').not().and(tweet('in_reply_to_status_id').eq(null));
                              }
                              else if (search.options.exclude_retweets) {
                                return tweet.hasFields('retweeted_status').not()
                    
                              }
                              else if (search.options.exclude_replys) {
                                return tweet('in_reply_to_status_id').eq(null);                
                              }
                              else {
                                return tweet;
                              }
                            })
                            .count() 
          }
        })
        .orderBy(r.desc('tweets_count'))
        .limit(10)

      query = query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        socket.emit('users_tweet_count', result);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      })
    })
  })

  socket.on('req_users_tweet_count', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query = r.table('tweets')
        .getAll(r.args(socket.tweets))
        .group(tweet => tweet('user')('id_str'))
        .count()
        .ungroup()
        .orderBy(r.desc('reduction'))
        .limit(10)
      query = query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        var topUsers = result.map(u => u.group);
        var topTweetsCount = result.map(u => u.reduction);
          var query = 
          r.expr(topUsers)
            .map(topTweetsCount, (user, tweetsCount) => {
              return (
              r.table('users')
                .get(user)
                .pluck('id_str', 'name','tweets_count' ,'screen_name', 'profile_image_url_https')
                .merge({ 'tweets_count': tweetsCount })
              )
            })
          return query = query.run(conn)
      })
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        socket.emit('users_tweet_count', result);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      })
    })
  })

  socket.on('req_users_tweet_count_distrib_overall', (groups) => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var dayCount = 30;
      var query = r.expr(socket.users)
        .map(user => {
          return (
            r.table('tweets')
            .getAll(user, {index: 'user_id'})
            .count()
          );
        })

      query = query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        let tweetCountDistrib = {
          groups: Array(groups.length).fill(0),
          average: 0,
        };
        if (result.length > 0) {
          tweetCountDistrib.groups = utils.groupBy(result, groups);
          tweetCountDistrib.average = Math.round(result.reduce((sum, x) => sum += x) / result.length); 
        }
        socket.emit('users_tweet_count_distrib', tweetCountDistrib);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      })
    })
  })

  socket.on('req_users_tweet_count_distrib', (groups) => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query = r.table('tweets')
        .getAll(r.args(socket.tweets))
        .group(tweet => tweet('user')('id_str'))
        .count()
        .ungroup()
        .getField('reduction')

      query = query.run(conn)
      .then(cursor => { return cursor.toArray(); })
      .then(result => {
        let tweetCountDistrib = {
          groups: Array(groups.length).fill(0),
          average: 0,
        };
        if (result.length > 0 && socket.users.length > 0) {
          tweetCountDistrib.groups = utils.groupBy(result, groups);
          tweetCountDistrib.average = Math.round(result.reduce((sum, x) => sum += x) / socket.users.length); 
        }
        socket.emit('users_tweet_count_distrib', tweetCountDistrib);
      })
      .error(err => { console.log("Failure:", err); })
      .finally(() => {
        if (conn)
          conn.close();
      })
    })
  })

  socket.on('req_tweet_structure_tweettype', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query1 = r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .filter(tweet => tweet.hasFields('retweeted_status'))
        .count()
        .run(conn);
      var query2 = r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .filter(tweet => tweet('in_reply_to_status_id').eq(null).not())
        .count()
        .run(conn);
      return q.all([query1, query2]);
    })
    .then(result => {
      const structuredCount= {
        tweet: socket.tweets.length - result[0] - result[1],
        retweet: result[0],
        reply: result[1],
      }
      socket.emit('tweet_structure_tweettype', structuredCount);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_tweet_structure_media', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      var query1 = r
      .table('tweets')
      .getAll(r.args(socket.tweets))
      .filter(tweet => {
        return (
          r.branch(tweet.hasFields('retweeted_status'),
            r.branch(tweet.hasFields({'retweeted_status':'extended_tweet'}), 
              tweet.hasFields({'retweeted_status' : { 'extended_tweet' : { 'extended_entities' : 'media'}}}),
              tweet.hasFields({'retweeted_status': { 'extended_entities' : 'media'}}),
              ),
            r.branch(tweet.hasFields('extended_tweet'), 
              tweet.hasFields({ 'extended_tweet' : { 'extended_entities' : 'media'}}),
              tweet.hasFields({ 'extended_entities' : 'media'}),
            ),
          ).not()
        );
      })
      .count()
      .run(conn);
      var query2 = r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .filter(tweet => {
          return (
          	r.branch(tweet.hasFields('retweeted_status'),
              r.branch(tweet.hasFields({'retweeted_status':'extended_tweet'}), 
                tweet('retweeted_status')('extended_tweet')('extended_entities')('media')('type').contains('photo'),
                tweet('retweeted_status')('extended_entities')('media')('type').contains('photo')
              ),
              r.branch(tweet.hasFields('extended_tweet'), 
                tweet('extended_tweet')('extended_entities')('media')('type').contains('photo'),
                tweet('extended_entities')('media')('type').contains('photo')
              ),
            )
          );
        })
        .count()
        .run(conn);
      return q.all([query1, query2]);
    })
    .then(result => {
      const structuredCount= {
        text: result[0],
        photo: result[1],
        video_or_gif: socket.tweets.length - result[0] - result[1],     
      }
      socket.emit('tweet_structure_media', structuredCount);
      
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_top_entities_hashtags', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      return (r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .concatMap(tweet => {
          return r.branch(tweet.hasFields('retweeted_status'),
            r.branch(
              tweet.hasFields({'retweeted_status': 'extended_tweet'}),
              tweet('retweeted_status')('extended_tweet')('entities')('hashtags')('text'),
              tweet('retweeted_status')('entities')('hashtags')('text')
            ),
            r.branch(
              tweet.hasFields('extended_tweet'),
              tweet('extended_tweet')('entities')('hashtags')('text'),
              tweet('entities')('hashtags')('text')
            )
          )
        })
        .group(tag => tag.downcase())
        .count()
        .ungroup()
        .orderBy(r.desc('reduction'))
        .limit(10)
        .run(conn)
      );
    })
    .then(result => {
      socket.emit('top_entities_hashtags', result);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_top_entities_urls', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      return (r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .concatMap(tweet => {
          return r.branch(tweet.hasFields('retweeted_status'),
            r.branch(
              tweet.hasFields({'retweeted_status': 'extended_tweet'}),
              tweet('retweeted_status')('extended_tweet')('entities')('urls')('display_url'),
              tweet('retweeted_status')('entities')('urls')('display_url')
            ),
            r.branch(
              tweet.hasFields('extended_tweet'),
              tweet('extended_tweet')('entities')('urls')('display_url'),
              tweet('entities')('urls')('display_url')
            )
          )
        })
        .group(url => url.downcase().match("(?im)^[a-zA-Z0-9-_\.]+")('str')).count()
        .ungroup().orderBy(r.desc('reduction'))
        .limit(10)
        .run(conn)
      );
    })
    .then(result => {
      socket.emit('top_entities_urls', result);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_usergroup_structure', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      return (r
        .table('users')
        .getAll(r.args(socket.users))
        .hasFields('list_tags')
        .concatMap(tweet => tweet('list_tags')('text'))
        .group(tag => tag)
        .count()
        .div(socket.users.length).mul(100).round()
        .ungroup()
        .orderBy(r.desc('reduction'))
        .limit(10)
        .run(conn)
      );
    })
    .then(result => {
      socket.emit('usergroup_structure', result);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_tweet_interaction', search => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      return (r
        .table('tweets')
        .reduce((agg, item) => {
          return {
            favorite_count: agg('favorite_count').add(item('favorite_count')),
            retweet_count: agg('retweet_count').add(item('retweet_count'))
            }
          })
        .do(result => {
          return r.object('favorite_count', result('favorite_count').div(socket.tweets.length).round(), 'retweet_count', result('retweet_count').div(socket.tweets.length).round())
          })
        .run(conn)
      );
    })
    .then(result => {
      result.total_tweet_count = socket.tweets.length;
      socket.emit('tweet_interaction', result);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_tweets_like_distrib', groups => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      return (r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .filter(tweet => tweet.hasFields('retweeted_status').not())
        .getField('favorite_count')
        .coerceTo('array')
        .run(conn)
      );
    })
    .then(result => {
      let likeDistrib = {
        groups: Array(groups.length).fill(0),
        average: 0,
      }
      if (result.length > 0) {
        likeDistrib.groups = utils.groupBy(result, groups);
        likeDistrib.average = Math.round(result.reduce((sum, x) => sum += x) / result.length);
      }
      return socket.emit('tweets_like_distrib', likeDistrib);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })

  socket.on('req_tweets_retweet_distrib', groups => {
    let conn;
    r.connect(config.database).then(c => {
      conn = c;
      return (r
        .table('tweets')
        .getAll(r.args(socket.tweets))
        .filter(tweet => tweet.hasFields('retweeted_status').not())
        .getField('retweet_count')
        .coerceTo('array')
        .run(conn)
      );
    })
    .then(result => {
      let retweetDistrib = {
        groups: Array(groups.length).fill(0),
        average: 0,
      }
      if (result.length > 0) {
        retweetDistrib.groups = utils.groupBy(result, groups);
        retweetDistrib.average = Math.round(result.reduce((sum, x) => sum += x) / result.length); 
      }
      return socket.emit('tweets_retweet_distrib', retweetDistrib);
    })
    .error(err => { console.log("Failure:", err); })
    .finally(() => {
      if (conn)
        conn.close();
    })
  })


})



// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

/**
 * Normalize a port into a number, string, or false.
 */

function normalizePort(val) {
  var port = parseInt(val, 10);

  if (isNaN(port)) {
    // named pipe
    return val;
  }

  if (port >= 0) {
    // port number
    return port;
  }

  return false;
}

/**
 * Event listener for HTTP server "error" event.
 */

function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }

  var bind = typeof port === 'string'
    ? 'Pipe ' + port
    : 'Port ' + port;

  // handle specific listen errors with friendly messages
  switch (error.code) {
    case 'EACCES':
      console.error(bind + ' requires elevated privileges');
      process.exit(1);
      break;
    case 'EADDRINUSE':
      console.error(bind + ' is already in use');
      process.exit(1);
      break;
    default:
      throw error;
  }
}

/**
 * Event listener for HTTP server "listening" event.
 */

function onListening() {
  var addr = http.address();
  var bind = typeof addr === 'string'
    ? 'pipe ' + addr
    : 'port ' + addr.port;
  debug('Listening on ' + bind);
}


module.exports = app;
