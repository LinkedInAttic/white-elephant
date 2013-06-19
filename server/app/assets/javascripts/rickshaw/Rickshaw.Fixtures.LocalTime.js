// A modified version of Rickshaw.Fixtures.Time.  It uses moment.js to perform ceil, which is more effective.
// It uses local time instead of UTC.

Rickshaw.namespace('Rickshaw.Fixtures.LocalTime');

Rickshaw.Fixtures.LocalTime = function() {

  var self = this;

  this.months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  this.units = [
    {
      name: 'decade',
      seconds: 86400 * 365.25 * 10,
      formatter: function(d) { return (parseInt(d.getFullYear() / 10) * 10) }
    }, {
      name: 'year',
      seconds: 86400 * 365.25,
      formatter: function(d) { return d.getFullYear() }
    }, {
      name: 'quarter',
      seconds: 86400 * 30.5*3,
      formatter: function(d) { 
        var quarter = "Q" + (1 + Math.floor(d.getMonth()/3))
        return quarter + " " + d.getFullYear();
    }
    },{
      name: 'month',
      seconds: 86400 * 30.5,
      formatter: function(d) { return self.months[d.getMonth()] }
    }, {
      name: 'week',
      seconds: 86400 * 7,
      formatter: function(d) { return self.formatDate(d) }
    }, {
      name: 'day',
      seconds: 86400,
      formatter: function(d) { return d.getDate() }
    }, {
      name: '6 hour',
      seconds: 3600 * 6,
      formatter: function(d) { return self.formatTime(d) }
    }, {
      name: 'hour',
      seconds: 3600,
      formatter: function(d) { return self.formatTime(d) }
    }, {
      name: '15 minute',
      seconds: 60 * 15,
      formatter: function(d) { return self.formatTime(d) }
    }, {
      name: 'minute',
      seconds: 60,
      formatter: function(d) { return d.getMinutes() }
    }, {
      name: '15 second',
      seconds: 15,
      formatter: function(d) { return d.getSeconds() + 's' }
    }, {
      name: 'second',
      seconds: 1,
      formatter: function(d) { return d.getSeconds() + 's' }
    }
  ];

  this.unit = function(unitName) {
    return this.units.filter( function(unit) { return unitName == unit.name } ).shift();
  };

  this.formatDate = function(d) {
    return d.toDateString();
  };

  this.formatTime = function(d) {
    return d.toTimeString();
  };

  this.ceil = function(time, unit) {

    var nearFuture = moment(new Date((time + unit.seconds - 1) * 1000));

    if (unit.name == 'week') {
      var rounded = nearFuture.day(0).startOf('day'); // set to Sunday
      return rounded.valueOf() / 1000;
    }

    if (unit.name == 'day') {
      var rounded = nearFuture.startOf('day');      
      return rounded.valueOf() / 1000;
    }
 
    if (unit.name == 'month') {
      var rounded = nearFuture.startOf('month');      
      return rounded.valueOf() / 1000;
    }

    if (unit.name == 'quarter') {
      var rounded = nearFuture.startOf('month');    
      // round down to beginning of quarter
      nearFuture.subtract('months',nearFuture.month()%3);
      return rounded.valueOf() / 1000;
    }

    if (unit.name == 'year') {
      var rounded = nearFuture.startOf('year');      
      return rounded.valueOf() / 1000;
    }

    return Math.ceil(time / unit.seconds) * unit.seconds;
  };
};
