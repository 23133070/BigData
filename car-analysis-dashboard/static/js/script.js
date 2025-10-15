async function loadData(endpoint, tableId) {
    const table = document.getElementById(tableId);
    if (!table) return;

    const tableBody = table.querySelector('tbody');
    const tableHead = table.querySelector('thead');

    // Hiển thị thông báo tải
    tableBody.innerHTML = '<tr><td colspan="100%">Đang tải dữ liệu...</td></tr>';

    try {
        const response = await fetch(endpoint);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();

        tableBody.innerHTML = '';
        tableHead.innerHTML = ''; // Xóa tiêu đề cũ

        if (data.length > 0) {
            // Lấy header từ keys của đối tượng đầu tiên
            const headers = Object.keys(data[0]);

            // Tạo hàng tiêu đề bảng
            tableHead.innerHTML = `<tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr>`;

            // Điền dữ liệu vào bảng
            data.forEach(row => {
                const tr = document.createElement('tr');
                tr.innerHTML = headers.map(h => `<td>${row[h] !== null ? row[h] : ''}</td>`).join('');
                tableBody.appendChild(tr);
            });
        } else {
            // Trường hợp không có dữ liệu
            tableBody.innerHTML = '<tr><td colspan="100%">Không có dữ liệu để hiển thị.</td></tr>';
        }

    } catch (error) {
        console.error(`Lỗi khi tải dữ liệu từ ${endpoint}:`, error);
        tableBody.innerHTML = `<tr><td colspan="100%">Lỗi: Không thể kết nối hoặc truy vấn dữ liệu. Vui lòng kiểm tra Server.</td></tr>`;
    }
}

function setupButtonControls() {
    const buttons = document.querySelectorAll('.control-button');

    buttons.forEach(button => {
        button.addEventListener('click', () => {
            // Ẩn tất cả các bảng (section)
            document.querySelectorAll('.data-section').forEach(section => {
                section.classList.remove('active-section');
            });

            // Bỏ trạng thái active khỏi tất cả các nút
            buttons.forEach(btn => btn.classList.remove('active'));

            // Lấy ID bảng mục tiêu (target)
            const targetId = button.getAttribute('data-target');
            const targetSection = document.getElementById(targetId);

            // Hiển thị bảng mục tiêu và đặt trạng thái active cho nút
            if (targetSection) {
                targetSection.classList.add('active-section');
                button.classList.add('active');
            }
        });
    });
}

document.addEventListener('DOMContentLoaded', () => {
    // Tải dữ liệu cho TẤT CẢ các bảng khi trang được tải (sau đó chỉ bảng đầu tiên được hiển thị)
    loadData('/api/avg_price_by_origin', 'table-origin');
    loadData('/api/avg_price_by_year', 'table-year-avg');
    loadData('/api/max_min_price_by_year', 'table-year-maxmin');
    loadData('/api/top_5_cars', 'table-top5');

    // Thiết lập logic chuyển đổi nút bấm
    setupButtonControls();
});