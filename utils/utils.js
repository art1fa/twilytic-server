function groupBy(arr, groups) {
  const result = Array(groups.length + 1).fill(0);

  arr.forEach(x => {
    if (x < groups[0]) {
      result[0] += 1;
      return;
    }
    if (x > groups[groups.length - 1]) {
      result[groups.length] += 1;
      return;
    }
    for (let i = 0; i < groups.length - 1; i += 1) {
      if (x >= groups[i] && x < groups[i+1]) {
        result[i + 1] += 1;
        break;
      }
    }
  });

  return result;
}

function alphanumSort(a, b) {
  function chunkify(t) {
    var tz = new Array();
    var x = 0, y = -1, n = 0, i, j;

    while (i = (j = t.charAt(x++)).charCodeAt(0)) {
      var m = (i == 46 || (i >=48 && i <= 57));
      if (m !== n) {
        tz[++y] = "";
        n = m;
      }
      tz[y] += j;
    }
    return tz;
  }

  var aa = chunkify(a);
  var bb = chunkify(b);

  for (x = 0; aa[x] && bb[x]; x++) {
    if (aa[x] !== bb[x]) {
      var c = Number(aa[x]), d = Number(bb[x]);
      if (c == aa[x] && d == bb[x]) {
        return c - d;
      } else return (aa[x] > bb[x]) ? 1 : -1;
    }
  }
  return aa.length - bb.length;
}

module.exports.groupBy = groupBy;
module.exports.alphanumSort = alphanumSort;