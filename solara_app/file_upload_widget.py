"""
Custom file upload widget using anywidget.
Provides both click-to-browse and drag-and-drop file upload.
"""

import anywidget
import traitlets


class FileUploadWidget(anywidget.AnyWidget):
    _esm = """
    function render({ model, el }) {
        const label = model.get("label") || "Drop file here or click to browse";
        const accept = model.get("accept") || ".zip";

        el.innerHTML = `
            <div class="file-upload-zone" style="
                position: relative;
                min-height: 60px;
                border: 1.5px dashed rgba(148,163,184,0.4);
                border-radius: 14px;
                padding: 12px;
                text-align: center;
                cursor: pointer;
                transition: border-color 0.25s, background 0.25s, box-shadow 0.25s;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 12px;
                color: #94a3b8;
                background: rgba(15,23,42,0.4);
            ">
                <input type="file" accept="${accept}" style="
                    position: absolute;
                    top: 0; left: 0;
                    width: 100%; height: 100%;
                    opacity: 0;
                    cursor: pointer;
                    z-index: 2;
                " />
                <span class="upload-label">${label}</span>
            </div>
        `;

        const zone = el.querySelector('.file-upload-zone');
        const input = el.querySelector('input[type="file"]');
        const labelSpan = el.querySelector('.upload-label');

        // Hover effects
        zone.addEventListener('dragover', (e) => {
            e.preventDefault();
            zone.style.borderColor = 'rgba(91,130,247,0.5)';
            zone.style.background = 'rgba(74,112,235,0.1)';
            zone.style.boxShadow = '0 0 20px rgba(99,140,255,0.1)';
        });
        zone.addEventListener('dragleave', () => {
            zone.style.borderColor = 'rgba(148,163,184,0.4)';
            zone.style.background = 'rgba(15,23,42,0.4)';
            zone.style.boxShadow = 'none';
        });

        // Handle drop
        zone.addEventListener('drop', async (e) => {
            e.preventDefault();
            zone.style.borderColor = 'rgba(148,163,184,0.4)';
            zone.style.background = 'rgba(15,23,42,0.4)';
            zone.style.boxShadow = 'none';
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                await readAndSend(files[0]);
            }
        });

        // Handle click-to-browse
        input.addEventListener('change', async (e) => {
            const files = e.target.files;
            if (files.length > 0) {
                await readAndSend(files[0]);
            }
        });

        async function readAndSend(file) {
            labelSpan.textContent = `Reading ${file.name}...`;
            try {
                const buffer = await file.arrayBuffer();
                const bytes = new Uint8Array(buffer);
                model.set("file_name", file.name);
                model.set("file_size", file.size);
                model.set("file_data", bytes);
                model.set("upload_trigger", model.get("upload_trigger") + 1);
                model.save_changes();
                labelSpan.textContent = file.name;
                zone.style.borderColor = 'rgba(74,222,128,0.5)';
                zone.style.background = 'rgba(74,222,128,0.05)';
            } catch (err) {
                labelSpan.textContent = `Error: ${err.message}`;
                zone.style.borderColor = 'rgba(255,92,108,0.5)';
                zone.style.background = 'rgba(255,92,108,0.05)';
            }
        }

        model.on("change:label", () => {
            if (!model.get("file_name")) {
                labelSpan.textContent = model.get("label");
            }
        });
    }
    export default { render };
    """

    _css = ""

    label = traitlets.Unicode("Drop file here or click to browse").tag(sync=True)
    accept = traitlets.Unicode(".zip").tag(sync=True)
    file_name = traitlets.Unicode("").tag(sync=True)
    file_size = traitlets.Int(0).tag(sync=True)
    file_data = traitlets.Bytes(b"").tag(sync=True)
    upload_trigger = traitlets.Int(0).tag(sync=True)
