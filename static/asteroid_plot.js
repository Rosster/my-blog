const moon_orbit = 384400;
const charts = {}

function hexToRgb(hex) {
  // Pilfered from: https://stackoverflow.com/questions/5623838/rgb-to-hex-and-hex-to-rgb
  // Expand shorthand form (e.g. "03F") to full form (e.g. "0033FF")
  let shorthandRegex = /^#?([a-f\d])([a-f\d])([a-f\d])$/i;
  hex = hex.replace(shorthandRegex, function(m, r, g, b) {
    return r + r + g + g + b + b;
  });

  let result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16)
  } : null;
}

function parse_data(asteroid_data, background_alpha, border_alpha){
    background_alpha = background_alpha || 1.0;
    border_alpha = border_alpha || 1.0;

    let data = asteroid_data;
    data.labels = [];
    for (let dataset of data.datasets) {
        dataset.backgroundColor = [];
        dataset.borderColor = [];
        let min_speed = Math.min.apply(null,dataset.data.map(d=>d.speed));
        let max_speed = Math.max.apply(null,dataset.data.map(d=>d.speed));

        for (let datapoint of dataset.data) {
            datapoint.r = Math.log(datapoint.width);
            datapoint.x = new Date(datapoint.x * 1000);
            data.labels.push(datapoint.x);

            let interpolation_func;
            if (dataset.label === 'safe') {
                interpolation_func = d3.interpolateYlOrBr;
            } else {
                interpolation_func = d3.interpolateYlOrRd;
            }
            let backgroundColor = interpolation_func((datapoint.speed-min_speed)/(max_speed-min_speed));
            let borderColor = interpolation_func((datapoint.speed-min_speed)/(max_speed-min_speed));

            let backgroundColorRGBA, borderColorRGBA
            if (backgroundColor[0] === '#') {
                backgroundColor = hexToRgb(backgroundColor);
                backgroundColorRGBA = `rgba(${backgroundColor.r},${backgroundColor.g},${backgroundColor.b},${background_alpha})`
            } else {
                backgroundColorRGBA = `${backgroundColor.replace(/rgb\(/gi, 'rgba(').slice(0, -1)},${background_alpha})`
            }

            if (borderColor[0] === '#') {
                borderColor = hexToRgb(borderColor);
                borderColorRGBA = `rgba(${borderColor.r},${borderColor.g},${borderColor.b},${border_alpha})`
            } else {
                borderColorRGBA = `${borderColor.replace(/rgb\(/gi, 'rgba(').slice(0, -1)},${border_alpha})`
            }

            dataset.backgroundColor.push(backgroundColorRGBA);
            dataset.borderColor.push(borderColorRGBA);

        }
    }
    return data
}

function update_chart(){
    if (charts.asteroids) {
        let curr_min_ts = charts.asteroids.options.scales.xAxes[0].ticks.min;
        let curr_max_ts = charts.asteroids.options.scales.xAxes[0].ticks.max;
        let new_min_ts = new Date();
        let delta_ms = new_min_ts.getTime() - curr_min_ts.getTime();
        charts.asteroids.options.scales.xAxes[0].ticks.min = new_min_ts;
        charts.asteroids.options.scales.xAxes[0].ticks.max = new Date(curr_max_ts.getTime() + delta_ms);
        charts.asteroids.update();
    }
}


document.addEventListener("DOMContentLoaded", function(event) {

    fetch("/internal/asteroid_plot_data").then(
        r => r.json()
    ).then((response)=>{
        let data = parse_data(response, .6, .5);

        // We expect two weeks, but we'll just show one so we can soothingly scroll
        let min_date = new Date();  // We make this now!
        let max_date = new Date(Math.max.apply(null, data.labels));
        let middle_date = new Date(min_date.getTime() + (max_date.getTime() - min_date.getTime()) / 2);

        let ctx = document.getElementById('myChart').getContext('2d');
        charts['asteroids'] = new Chart(ctx, {
            type: 'bubble',
            data: data,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                tooltips: {
                    callbacks: {
                        label: function (tooltipItem, data) {
                            let asteroid_data = data.datasets[tooltipItem.datasetIndex].data[tooltipItem.index];
                            let asteroid_name = asteroid_data.name;
                            let asteroid_speed = asteroid_data.speed;
                            let asteroid_width = Math.round(asteroid_data.width);
                            return `${asteroid_name}, ${asteroid_width} m @ ${Math.round((asteroid_speed + Number.EPSILON) * 100) / 100} km/s`
                        }
                    }
                },
                title: {
                    display: true,
                    text: 'Local Asteroids'
                },
                scales: {
                    yAxes: [{
                        type: 'logarithmic',
                        scaleLabel: {
                            display: true,
                            labelString: "Distance to Earth (km)"
                        },
                        gridLines: {
                            display:false,
                        }
                    }],
                    xAxes: [{
                        ticks: {
                            userCallback: function (label, index, labels) {
                                let dt = new Date(label);
                                return dt.toLocaleString().split(', ')
                            },
                            max: middle_date,
                            min: min_date,
                            stepSize: (middle_date - min_date) / 5
                        },
                        scaleLabel: {
                            display: true,
                            labelString: "Time of Nearest Approach"
                        },
                        gridLines: {
                            display:false,
                        }
                    }]

                },
                annotation: {
                    annotations: [{
                        type: 'line',
                        mode: 'horizontal',
                        scaleID: 'y-axis-0',
                        value: moon_orbit,
                        borderColor: 'rgba(218, 184, 148, 0.5)',
                        borderWidth: 2,
                        label: {
                            enabled: true,
                            content: 'Orbit of the Moon',
                            backgroundColor: 'rgba(0,0,0,0)',
                            fontColor: '#111111',
                            fontStyle: 'sans-serif'
                        }
                    }]
                }
            }

        })

        window.setInterval(update_chart, 5000);

    })
});
