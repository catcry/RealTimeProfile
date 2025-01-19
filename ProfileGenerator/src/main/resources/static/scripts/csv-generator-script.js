function toggleModeDivs(type) {
    const randomModeDiv = document.getElementById('random-mode');
    const customModeDiv = document.getElementById('custom-mode');
    document.getElementById("entities-table").style = 'display: none'

    if (type === 'random') {
        randomModeDiv.classList.remove('hidden');
        randomModeDiv.classList.add('visible');
        customModeDiv.classList.remove('visible');
        customModeDiv.classList.add('hidden');
    } else if (type === 'custom') {
        customModeDiv.classList.remove('hidden');
        customModeDiv.classList.add('visible');
        randomModeDiv.classList.remove('visible');
        randomModeDiv.classList.add('hidden');
    }
}

let fetchForm = document.getElementById("custom-csv-form");
function createCustomCsv(event) {
    event.preventDefault();

    console.log("event.target in form")
    const formData = new FormData(fetchForm);
    const jsonData = {};
    formData.forEach((value, key) => jsonData[key] = value);

    fetch('/create-custom-csv' +
        "?remoteAddress=" + document.getElementById("remoteAddress").value, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(jsonData)
    })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            console.log(response);
            return response.json();
        })
        .then(data => {
            console.log("data received");
            document.getElementById("entities-table").style = 'display: grid'
            const tableBody = document.getElementById('table-body');
            tableBody.innerHTML = '';
            data.forEach(entity => {
                const row = document.createElement('tr');
                const fields = [
                    entity.eventType, entity.eventId, entity.callingNumber, entity.callingNumberType,
                    entity.calledNumber, entity.calledNumberType, entity.callingCountry, entity.calledCountry,
                    entity.eventDuration, entity.eventVolume, entity.internationalCall, entity.onnet,
                    entity.isRoaming, entity.eventStartTimestamp, entity.pkgStartTime, entity.pkgEndTime,
                    entity.pkgNum, entity.subBalance, entity.cost, entity.otherNetwork, entity.paygVoice
                ];
                fields.forEach(field => {
                    const cell = document.createElement('td');
                    cell.textContent = field;
                    row.appendChild(cell);
                });
                tableBody.appendChild(row);
            });
            console.log("before success alert");
            alert('Form submitted successfully!');
        })
        .catch((error) => {
            console.error('Error:', error);
            alert('Form submission failed.');
        });
}
fetchForm.addEventListener('submit', createCustomCsv);

function fetchEntities() {
    fetch('/api/entities'
        + '?count=' + document.getElementById("count").value
        + "&remoteAddress=" + document.getElementById("remoteAddress").value
        + "&useReadyProfile=" + document.getElementById("useReadyProfile").checked)
        .then(response => response.json())
        .then(data => {
            document.getElementById("entities-table").style = 'display: grid'
            const tableBody = document.getElementById('table-body');
            tableBody.innerHTML = '';
            data.forEach(entity => {
                const row = document.createElement('tr');
                const fields = [
                    entity.eventType, entity.eventId, entity.callingNumber, entity.callingNumberType,
                    entity.calledNumber, entity.calledNumberType, entity.callingCountry, entity.calledCountry,
                    entity.eventDuration, entity.eventVolume, entity.internationalCall, entity.onnet,
                    entity.isRoaming, entity.eventStartTimestamp, entity.pkgStartTime, entity.pkgEndTime,
                    entity.pkgNum, entity.subBalance, entity.cost, entity.otherNetwork, entity.paygVoice
                ];
                fields.forEach(field => {
                    const cell = document.createElement('td');
                    cell.textContent = field;
                    row.appendChild(cell);
                });
                tableBody.appendChild(row);
            });
        });
}
