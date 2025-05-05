// server.js
const express = require('express');
const multer = require('multer');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const cors = require('cors');
const SSE = require('express-sse'); // 用于 Server-Sent Events
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3001;

// --- 目录配置 ---
const baseDir = __dirname; // Get directory of server.js
const uploadDir = path.join(baseDir, 'uploads');
const outputDir = path.join(baseDir, 'analyzed_results');
const pythonScriptPath = path.join(baseDir, 'run_analysis_workflow.py');

// 获取 Python 解释器的完整路径 (从环境变量 PYTHON_EXECUTABLE 获取，或默认使用 'python')
const pythonExecutablePath = process.env.PYTHON_EXECUTABLE || 'python';
if (pythonExecutablePath === 'python') {
    console.warn('PYTHON_EXECUTABLE environment variable is not set. Using "python" command. Ensure "python" is in the system PATH.');
} else {
     console.log(`Python executable path: ${pythonExecutablePath}`);
}


// 确保目录存在
fs.existsSync(uploadDir) || fs.mkdirSync(uploadDir, { recursive: true });
fs.existsSync(outputDir) || fs.mkdirSync(outputDir, { recursive: true });

// --- 文件上传配置 ---
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, uploadDir); // 将文件保存在 ./uploads 目录下
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now();
    // Use original base name + timestamp + original extension
    cb(null, `${path.parse(file.originalname).name}-${uniqueSuffix}${path.extname(file.originalname)}`);
  }
});
const upload = multer({ storage: storage, limits: { fileSize: 1024 * 1024 * 50 } }); // Limit file size to 50MB

// --- SSE 实例 ---
const sse = new SSE(["message"]); // Initial data if needed


// --- 中间件 ---
app.use(cors()); // Allow cross-origin requests (adjust origin in production)
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// --- 路由 ---

app.get('/', (req, res) => {
  res.send('Chat Analysis Backend is running!');
});

// File upload and analysis initiation endpoint
// 'chatFile' is the name attribute of the input type="file" in the frontend form
app.post('/upload_and_analyze', upload.single('chatFile'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }

  // TODO: Add logic to prevent multiple simultaneous analysis tasks if necessary


  const inputJsonPath = req.file.path; // Path where multer saved the temporary file
  const outputExcelFilename = `analysis_result_${Date.now()}_${path.parse(req.file.originalname).name}.xlsx`; // Generate a unique output filename
  const outputExcelPath = path.join(outputDir, outputExcelFilename); // Full path for the final Excel file

  // Get optional limit parameter from request body
  const limit = req.body.limit;
  const limitArgs = [];
  if (limit !== undefined && limit !== null && limit !== '') {
      const limitNum = parseInt(limit, 10);
      if (!isNaN(limitNum) && limitNum >= 0) {
          limitArgs.push(limitNum.toString());
      } else {
          console.warn(`Received invalid limit value: ${limit}. Ignoring limit.`);
          // Optionally send a warning via SSE here if stream is already initiated
      }
  }

  // Arguments to pass to the Python script
  const pythonArgs = [
    pythonScriptPath,    // Path to the Python workflow script
    inputJsonPath,       // Argument 1: Path to the input JSON file
    outputExcelPath,     // Argument 2: Path for the output Excel file
    ...limitArgs         // Argument 3 (optional): Limit value
  ];

  console.log(`Spawning Python process: ${pythonExecutablePath} ${pythonArgs.join(' ')}`);

  let pythonStdout = ''; // Buffer to collect all stdout for potential error reporting
  let pythonStderr = ''; // Buffer to collect all stderr for potential error reporting


  try {
      // Spawn the Python child process
      const pythonChildProcess = spawn(pythonExecutablePath, pythonArgs, {
          cwd: baseDir, // Set working directory to server.js directory
          env: { ...process.env, PYTHONIOENCODING: 'utf-8' } // Pass environment variables, ensure UTF-8 output from Python
      });

      // Capture and forward Python's stdout to SSE clients
      pythonChildProcess.stdout.on('data', (data) => {
        const dataString = data.toString();
        // console.log(`Python stdout: ${dataString.trim()}`); // Log to Node.js console
        pythonStdout += dataString; // Collect for final error report

        // Split by lines to process each log line
        dataString.split('\n').forEach(line => {
            if (line.startsWith('PYTHON_STATUS:')) {
                sse.send({ type: 'status', message: line.substring('PYTHON_STATUS:'.length).trim() }, 'message');
            } else if (line.startsWith('PYTHON_PARSED_CHAT_RESULT:')) {
                 // Send the successfully parsed JSON string to frontend
                 const jsonString = line.substring('PYTHON_PARSED_CHAT_RESULT:'.length).trim();
                 // Frontend will parse this string as JSON and display it
                 sse.send({ type: 'parsed_result', content: jsonString }, 'message');
            } else if (line.startsWith('PYTHON_RESULT_JSON:')) {
                 // Send final aggregated JSON result (if printed by Python)
                 const jsonContent = line.substring('PYTHON_RESULT_JSON:'.length).trim();
                 try {
                     const finalResult = JSON.parse(jsonContent);
                     sse.send({ type: 'final_result_json', content: finalResult }, 'message');
                 } catch (e) {
                     console.error('Failed to parse final JSON result from stdout:', e);
                     sse.send({ type: 'log_error', message: `Failed to parse final JSON result: ${e}. Raw content: ${jsonContent.substring(0, 200)}...` }, 'message');
                 }
            } else {
                 // Forward other standard print statements as generic logs
                 if (line.trim()) {
                    sse.send({ type: 'log', message: line.trim() }, 'message');
                 }
            }
        });
      });

      // Capture and forward Python's stderr to SSE clients
      pythonChildProcess.stderr.on('data', (data) => {
        const dataString = data.toString();
        // console.error(`Python stderr: ${dataString.trim()}`); // Log to Node.js console
        pythonStderr += dataString; // Collect for final error report

        // Split by lines to process each log line
         dataString.split('\n').forEach(line => {
             if (line.startsWith('PYTHON_FATAL_ERROR:')) {
                 sse.send({ type: 'fatal_error', message: line.substring('PYTHON_FATAL_ERROR:'.length).trim(), details: pythonStderr }, 'message'); // Include all stderr in fatal error
             } else if (line.startsWith('PYTHON_ERROR:')) {
                  sse.send({ type: 'error', message: line.substring('PYTHON_ERROR:'.length).trim() }, 'message');
             } else if (line.startsWith('PYTHON_WARNING:')) {
                  sse.send({ type: 'warning', message: line.substring('PYTHON_WARNING:'.length).trim() }, 'message');
             } else if (line.trim()) {
                 // Forward other standard error print statements as generic error logs
                 sse.send({ type: 'log_error', message: line.trim() }, 'message');
             }
         });
      });

      // Listen for Python process exit
      pythonChildProcess.on('close', (code) => {
        console.log(`Python process exited with code ${code}`);

        // Clean up temporary uploaded JSON file
        fs.unlink(inputJsonPath, (err) => {
          if (err) console.error(`Error removing temporary input file ${inputJsonPath}:`, err);
          else console.log(`Temporary input file ${inputJsonPath} removed.`);
        });

        // Use collected stdout/stderr for final error report if needed
        const finalStdout = pythonStdout;
        const finalStderr = pythonStderr;


        if (code === 0) {
          // Python script finished successfully
          // Check if the expected output file was created
          if (fs.existsSync(outputExcelPath)) {
            // --- FIX: Send 'complete' as the SSE event name ---
            sse.send({ // Data payload for the 'complete' event
              status: 'Analysis complete',
              filename: outputExcelFilename,
              downloadUrl: `/download/${outputExcelFilename}`, // Relative URL for download
              // Optional: Include captured output in complete event data for debugging
              // stdout: finalStdout,
              // stderr: finalStderr
            }, 'complete'); // <-- THIS is the SSE event name
            // --------------------------------------------------------------------------
            console.log(`Analysis complete. Output file: ${outputExcelPath}`);
          } else {
            // Python exited with 0, but output file is missing - indicates a problem in Python's saving logic
            const errorMessage = 'Analysis completed (Python exit code 0), but output file was not found.';
            console.error(errorMessage);
             // --- Send an 'error' event if output file missing ---
             sse.send({
                 type: 'error', // Data payload type
                 message: errorMessage,
                 details: `Expected file: ${outputExcelPath}\n` + finalStdout + finalStderr
             }, 'error'); // <-- Send as 'error' event
             // ------------------------------------------------
          }
        } else {
          // Python script failed (non-zero exit code)
          const errorMessage = `Analysis failed during Python execution. Exit code: ${code}.`;
          console.error(errorMessage);
           // --- Send an 'error' event for process failure ---
           // Check if a fatal error was already reported by PythonStderr
           if (!finalStderr.includes('PYTHON_FATAL_ERROR:')) {
              sse.send({
                  type: 'error', // Data payload type
                  message: errorMessage,
                  details: finalStdout + finalStderr
              }, 'error'); // <-- Send as 'error' event
           }
           // If PYTHON_FATAL_ERROR was printed, it should have been sent via stderr handler already
           // The fatal_error event listener in frontend will handle setting isAnalyzing=false

           // If no fatal_error log was sent, ensure isAnalyzing=false is handled by error listener
        }

        // No sse.send('end') is needed here.
      });

      // Listen for process spawn errors (e.g., python command not found)
      pythonChildProcess.on('error', (err) => {
         const errorMessage = 'Failed to spawn python process. Check if python executable is in PATH or configured correctly via PYTHON_EXECUTABLE env var.';
         console.error(errorMessage, err);
         // Clean up input file in case of spawn error
         if (req.file && req.file.path && fs.existsSync(req.file.path)) {
             fs.unlink(req.file.path, (unlinkErr) => { if (unlinkErr) console.error(`Error removing temp file after spawn error:`, unlinkErr); });
         }
         // Send a fatal error event to the frontend
         sse.send({
             type: 'fatal_error', // Data payload type
             message: errorMessage,
             details: err.message
         }, 'fatal_error'); // <-- Send as 'fatal_error' event
      });


      // Send initial success response to the frontend immediately after spawning
      // This response does NOT signal analysis completion, only that the process started
      res.status(202).json({
          status: 'File uploaded and analysis initiated. Connect to /analysis_stream for progress.',
          outputFilename: outputExcelFilename // Send the expected output filename
      });

  } catch (error) {
      // Catch synchronous errors during file upload or initial spawn setup
      console.error('Synchronous error during upload/spawn:', error);
      // Clean up input file in case of synchronous error
      if (req.file && req.file.path && fs.existsSync(req.file.path)) {
          fs.unlink(req.file.path, (unlinkErr) => { if (unlinkErr) console.error(`Error removing temp file after sync error:`, unlinkErr); });
      }
      // Send error response to frontend
      res.status(500).json({ error: 'Server failed to initiate analysis.', details: error.message });
  }

});

// SSE connection endpoint
app.get('/analysis_stream', sse.init);


// File download endpoint
app.get('/download/:filename', (req, res) => {
  const filename = req.params.filename;
  // Sanitize filename to prevent directory traversal attacks
  const safeFilename = path.basename(filename);
  const filePath = path.join(outputDir, safeFilename);

  console.log(`Attempting to download file: ${filePath}`);

  // Check if the file exists and is within the output directory
  if (fs.existsSync(filePath) && filePath.startsWith(outputDir)) {
    res.setHeader('Content-Disposition', `attachment; filename="${safeFilename}"`);
    // Use res.sendFile for potentially larger files
    res.sendFile(filePath, (err) => {
      if (err) {
        console.error(`Error during file download ${filePath}:`, err);
        // Check if headers have already been sent before trying to send a new response
        if (!res.headersSent) {
             if (err.code === 'ENOENT') {
                 res.status(404).send('File not found.');
             } else {
                 res.status(500).send('Error downloading file.');
             }
        }
      } else {
          console.log(`File ${safeFilename} downloaded successfully.`);
      }
    });
  } else {
    console.warn(`File not found or not allowed for download: ${filePath}`);
    res.status(404).json({ error: 'File not found or access denied.' });
  }
});


// Start the Node.js server
app.listen(port, () => {
  console.log(`Node.js backend listening on port ${port}`);
  console.log(`GOOGLE_API_KEY loaded in Node.js: ${!!process.env.GOOGLE_API_KEY}`);
  // Verify Python script exists on startup
   if (!fs.existsSync(pythonScriptPath)) {
       console.error(`FATAL: Python workflow script not found at ${pythonScriptPath}. Analysis will fail.`);
   }
   console.log(`Using Python executable: ${pythonExecutablePath}`);

});