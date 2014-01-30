if (!window.Monitrix)
	window.Monitrix = {};

/**	
 * @param {number} millisSinceStart the time since the clock has started
 * @param {element} the HTML element which should be updated with the time
 * @constructor
 */
Monitrix.ProgressClock = function(millisSinceStart, element) {
  /** @private **/
  this.DAY_MS = 24 * 60 * 60 * 1000;
  
  /** @private **/
  this.HOUR_MS = 60 * 60 * 1000;
  
  /** @private **/
  this.MIN_MS = 60 * 1000;
  
  /** @private **/
  this.days;

  /** @private **/
  this.hours;
  
  /** @private **/
  this.minutes;
  
  /** @private **/
  this.seconds;
  
  this._update(millisSinceStart, element);

  var self = this;
  this.clock = window.setInterval(function() {
    millisSinceStart += 1000;
    self._update(millisSinceStart, element);
  }, 1000);  
}

Monitrix.ProgressClock.prototype._update = function(millisSinceStart, element) {
  this.days = Math.floor(millisSinceStart / this.DAY_MS);
  this.hours = Math.floor((millisSinceStart % this.DAY_MS) / this.HOUR_MS);
  this.minutes = Math.floor((millisSinceStart % this.HOUR_MS) / this.MIN_MS);
  this.seconds = Math.floor((millisSinceStart % this.MIN_MS) / 1000);
  element.html(this.days + ' days ' + this.hours + ' hrs ' + this.minutes + ' min ' + this.seconds + ' sec');
}

Monitrix.ProgressClock.prototype.stop = function() {

}